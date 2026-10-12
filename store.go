// Package ledgerkv is a tiny append-only-log key-value store with TTL,
// snapshot compaction and deterministic crash injection for tests.
package ledgerkv

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	opSet byte = 1
	opDel byte = 2

	logName      = "ledger.log"
	snapName     = "ledger.snap"
	snapTmpName  = "ledger.snap.tmp"
	headerSize   = 8 // crc32 + payload length
	maxRecordLen = 64 << 20
)

// ErrCrash is returned by injected failures. The store is unusable afterwards,
// exactly as if the process had died.
var ErrCrash = errors.New("ledgerkv: injected crash")

// ErrClosed is returned by writes after Close, an injected crash, or an I/O
// failure. Reads keep serving the in-memory state.
var ErrClosed = errors.New("ledgerkv: store closed")

// ErrTooLarge is returned when a key and value would not fit in one record.
var ErrTooLarge = errors.New("ledgerkv: record too large")

// errCorrupt marks data damage (bad checksum or length), as opposed to an
// I/O failure. Only data damage is treated as a torn tail during replay.
var errCorrupt = errors.New("corrupt record")

// Options configures Open. All fields are optional.
type Options struct {
	// Now supplies the clock. Defaults to time.Now.
	Now func() time.Time
	// SyncEveryWrite fsyncs the log after each mutation.
	SyncEveryWrite bool
	// CrashAfterBytes, when > 0, makes the log writer persist only that many
	// more bytes (a torn write) and then fail with ErrCrash.
	CrashAfterBytes int64
	// Hook is called at named points during Snapshot. Returning an error
	// simulates a crash at that point. Points: "snap-written",
	// "snap-renamed", "log-truncated".
	Hook func(point string) error
}

type entry struct {
	value    []byte
	expireAt int64 // unix nanos, 0 = never
}

// Store is safe for concurrent use.
type Store struct {
	mu      sync.RWMutex
	dir     string
	opts    Options
	data    map[string]entry
	log     *os.File
	dead    bool
	budget  int64 // remaining crash budget, -1 = unlimited
	logSize int64
}

// Open opens or creates a store in dir, recovering from snapshot and log.
func Open(dir string, opts Options) (*Store, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, opts: opts, data: map[string]entry{}, budget: -1}
	if opts.CrashAfterBytes > 0 {
		s.budget = opts.CrashAfterBytes
	}
	// A leftover tmp snapshot is an interrupted snapshot: discard it.
	_ = os.Remove(filepath.Join(dir, snapTmpName))
	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}
	good, err := s.replayLog()
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	// Drop any torn tail so new appends start on a record boundary.
	if err := f.Truncate(good); err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	s.log, s.logSize = f, good
	return s, nil
}

// recordFits reports whether a record for key and value is within maxRecordLen.
func recordFits(key string, value []byte) bool {
	return uint64(1)+4+uint64(len(key))+8+uint64(len(value)) <= maxRecordLen
}

func encode(op byte, key string, value []byte, expireAt int64) []byte {
	payload := make([]byte, 0, 1+4+len(key)+8+len(value))
	payload = append(payload, op)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(key)))
	payload = append(payload, key...)
	payload = binary.BigEndian.AppendUint64(payload, uint64(expireAt))
	payload = append(payload, value...)
	rec := make([]byte, headerSize, headerSize+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], crc32.ChecksumIEEE(payload))
	binary.BigEndian.PutUint32(rec[4:8], uint32(len(payload)))
	return append(rec, payload...)
}

// readRecord returns io.EOF at a clean end and io.ErrUnexpectedEOF or a
// checksum error for a torn or corrupt record.
func readRecord(r *bufio.Reader) (op byte, key string, value []byte, expireAt int64, n int64, err error) {
	var hdr [headerSize]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		return
	}
	sum, ln := binary.BigEndian.Uint32(hdr[0:4]), binary.BigEndian.Uint32(hdr[4:8])
	if ln < 13 || ln > maxRecordLen {
		err = fmt.Errorf("%w: bad record length %d", errCorrupt, ln)
		return
	}
	payload := make([]byte, ln)
	if _, err = io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}
	if crc32.ChecksumIEEE(payload) != sum {
		err = fmt.Errorf("%w: checksum mismatch", errCorrupt)
		return
	}
	op = payload[0]
	kl := binary.BigEndian.Uint32(payload[1:5])
	if uint64(5)+uint64(kl)+8 > uint64(ln) {
		err = fmt.Errorf("%w: bad key length", errCorrupt)
		return
	}
	key = string(payload[5 : 5+kl])
	expireAt = int64(binary.BigEndian.Uint64(payload[5+kl : 13+kl]))
	value = payload[13+kl:]
	n = int64(headerSize) + int64(ln)
	return
}

func (s *Store) loadSnapshot() error {
	f, err := os.Open(filepath.Join(s.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		op, k, v, exp, _, err := readRecord(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			// Snapshots are renamed into place only once complete, so
			// corruption here is real damage, not a torn write.
			return fmt.Errorf("snapshot corrupt: %w", err)
		}
		if op == opSet {
			s.data[k] = entry{value: v, expireAt: exp}
		}
	}
}

func (s *Store) replayLog() (good int64, err error) {
	f, err := os.Open(filepath.Join(s.dir, logName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		op, k, v, exp, n, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCorrupt) {
				return good, nil // clean EOF or torn tail: keep the valid prefix
			}
			return good, err // a real I/O error must never lead to truncation
		}
		switch op {
		case opSet:
			s.data[k] = entry{value: v, expireAt: exp}
		case opDel:
			delete(s.data, k)
		}
		good += n
	}
}

func (s *Store) appendRec(rec []byte) error {
	if s.dead {
		return ErrClosed
	}
	if s.budget >= 0 && int64(len(rec)) > s.budget {
		_, _ = s.log.Write(rec[:s.budget])
		s.budget = 0
		s.die()
		return ErrCrash
	}
	if n, err := s.log.Write(rec); err != nil || n != len(rec) {
		// A partial record may now sit at the tail. Fail closed: the caller
		// must reopen, and recovery truncates the torn record.
		s.die()
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("ledgerkv: log write failed, store closed: %w", err)
	}
	if s.budget >= 0 {
		s.budget -= int64(len(rec))
	}
	s.logSize += int64(len(rec))
	if s.opts.SyncEveryWrite {
		if err := s.log.Sync(); err != nil {
			// After a failed fsync the durable state is unknown.
			s.die()
			return fmt.Errorf("ledgerkv: log sync failed, store closed: %w", err)
		}
	}
	return nil
}

func (s *Store) die() {
	s.dead = true
	if s.log != nil {
		s.log.Close()
	}
}

func (s *Store) alive(e entry) bool {
	return e.expireAt == 0 || s.opts.Now().UnixNano() < e.expireAt
}

// Set stores value under key. A ttl <= 0 means no expiry.
func (s *Store) Set(key string, value []byte, ttl time.Duration) error {
	if !recordFits(key, value) {
		return ErrTooLarge
	}
	var exp int64
	if ttl > 0 {
		exp = s.opts.Now().Add(ttl).UnixNano()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendRec(encode(opSet, key, value, exp)); err != nil {
		return err
	}
	s.data[key] = entry{value: append([]byte(nil), value...), expireAt: exp}
	return nil
}

// Get returns the value and whether it exists and has not expired.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok || !s.alive(e) {
		return nil, false
	}
	return append([]byte(nil), e.value...), true
}

// TTL reports the remaining lifetime. ok is false if the key is missing or
// expired; a zero duration with ok true means no expiry.
func (s *Store) TTL(key string) (d time.Duration, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, found := s.data[key]
	if !found || !s.alive(e) {
		return 0, false
	}
	if e.expireAt == 0 {
		return 0, true
	}
	return time.Duration(e.expireAt - s.opts.Now().UnixNano()), true
}

// Delete removes key. Deleting a missing key is not an error.
func (s *Store) Delete(key string) error {
	if !recordFits(key, nil) {
		return ErrTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.appendRec(encode(opDel, key, nil, 0)); err != nil {
		return err
	}
	delete(s.data, key)
	return nil
}

// Len counts live (unexpired) keys.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, e := range s.data {
		if s.alive(e) {
			n++
		}
	}
	return n
}

// LogSize returns the current log size in bytes.
func (s *Store) LogSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logSize
}

func (s *Store) hook(p string) error {
	if s.opts.Hook == nil {
		return nil
	}
	if err := s.opts.Hook(p); err != nil {
		s.die()
		return fmt.Errorf("%w at %s", ErrCrash, p)
	}
	return nil
}

// Snapshot writes all live entries to a new snapshot (tmp file, fsync,
// atomic rename) and then truncates the log. Replaying the old log over the
// new snapshot is idempotent, so a crash between rename and truncate is safe.
func (s *Store) Snapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return ErrClosed
	}
	tmp := filepath.Join(s.dir, snapTmpName)
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for k, e := range s.data {
		if !s.alive(e) {
			delete(s.data, k) // expired entries are dropped at compaction
			continue
		}
		if _, err := w.Write(encode(opSet, k, e.value, e.expireAt)); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := s.hook("snap-written"); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, snapName)); err != nil {
		return err
	}
	// Make the rename durable before the log is truncated, otherwise a power
	// loss could persist the truncation but not the new snapshot.
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if err := s.hook("snap-renamed"); err != nil {
		return err
	}
	if err := s.log.Truncate(0); err != nil {
		return err
	}
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s.logSize = 0
	return s.hook("log-truncated")
}

// Close syncs and closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil
	}
	s.dead = true
	if err := s.log.Sync(); err != nil {
		s.log.Close()
		return err
	}
	return s.log.Close()
}

// syncDir fsyncs a directory so renames inside it are durable. Platforms that
// cannot open or sync directories (Windows) are tolerated.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		var pe *os.PathError
		if errors.As(err, &pe) {
			return nil // e.g. EINVAL on filesystems without directory fsync
		}
		return err
	}
	return nil
}
