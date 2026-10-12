package ledgerkv

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// An acknowledged write must never be dropped on reopen, so records the
// reader would refuse are rejected at write time instead.
func TestOversizedWriteRejected(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	defer s.Close()
	err := s.Set("oversize", bytes.Repeat([]byte("x"), maxRecordLen), 0)
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge, got %v", err)
	}
	if _, ok := s.Get("oversize"); ok {
		t.Fatal("rejected write became visible")
	}
	if err := s.Delete(string(bytes.Repeat([]byte("k"), maxRecordLen))); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized key delete: %v", err)
	}
}

func TestLargestAcceptedRecordSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	val := bytes.Repeat([]byte("v"), maxRecordLen-13-1)
	if err := s.Set("k", val, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("after", []byte("1"), 0); err != nil {
		t.Fatal(err)
	}
	s.Close()
	r := open(t, dir, Options{})
	defer r.Close()
	if v, ok := r.Get("k"); !ok || len(v) != len(val) {
		t.Fatal("largest record lost")
	}
	if _, ok := r.Get("after"); !ok {
		t.Fatal("record after the largest one lost")
	}
}

func TestWriteErrorFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	s.Set("a", []byte("1"), 0)
	s.log.Close() // make the next write fail with an I/O error
	if err := s.Set("b", []byte("2"), 0); err == nil {
		t.Fatal("expected write error")
	}
	if err := s.Set("c", []byte("3"), 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("store must stay closed, got %v", err)
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("failed write became visible")
	}
	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Fatal("reads should keep serving earlier state")
	}
	r := open(t, dir, Options{})
	defer r.Close()
	if _, ok := r.Get("a"); !ok {
		t.Fatal("a lost")
	}
}

// A real read error (here: the log path is a directory) must make Open fail,
// not be mistaken for a torn tail and truncated.
func TestIOErrorDuringReplayIsNotTornTail(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, logName), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("Open must fail on an unreadable log")
	}
}

func TestAfterCloseReadsServeWritesFail(t *testing.T) {
	s := open(t, t.TempDir(), Options{})
	s.Set("a", []byte("1"), 0)
	s.Close()
	if _, ok := s.Get("a"); !ok {
		t.Fatal("reads after Close should serve memory")
	}
	if err := s.Set("b", nil, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
	if err := s.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}
