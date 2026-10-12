package ledgerkv

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func open(t *testing.T, dir string, o Options) *Store {
	t.Helper()
	s, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBasicAndReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	s.Set("a", []byte("1"), 0)
	s.Set("b", []byte("2"), 0)
	s.Delete("a")
	s.Close()
	s = open(t, dir, Options{})
	if _, ok := s.Get("a"); ok {
		t.Fatal("a should be deleted")
	}
	if v, ok := s.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("b = %q %v", v, ok)
	}
}

func TestTTL(t *testing.T) {
	c := &clock{time.Unix(1000, 0)}
	dir := t.TempDir()
	s := open(t, dir, Options{Now: c.now})
	s.Set("k", []byte("v"), 10*time.Second)
	if d, ok := s.TTL("k"); !ok || d != 10*time.Second {
		t.Fatalf("ttl %v %v", d, ok)
	}
	c.t = c.t.Add(11 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("should be expired")
	}
	s.Close()
	// expiry survives restart because deadlines are absolute
	s = open(t, dir, Options{Now: c.now})
	if _, ok := s.Get("k"); ok || s.Len() != 0 {
		t.Fatal("expired key resurrected")
	}
}

func TestSnapshotCompacts(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	for i := 0; i < 100; i++ {
		s.Set("k", []byte(fmt.Sprint(i)), 0)
	}
	before := s.LogSize()
	if err := s.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if s.LogSize() != 0 || before == 0 {
		t.Fatal("log not truncated")
	}
	s.Set("z", []byte("last"), 0)
	s.Close()
	s = open(t, dir, Options{})
	if v, _ := s.Get("k"); string(v) != "99" {
		t.Fatalf("k=%s", v)
	}
	if v, _ := s.Get("z"); string(v) != "last" {
		t.Fatal("z lost")
	}
}

type op struct {
	del  bool
	k, v string
}

func workload(seed int64, n int) []op {
	r := rand.New(rand.NewSource(seed))
	ops := make([]op, n)
	for i := range ops {
		ops[i] = op{del: r.Intn(4) == 0, k: fmt.Sprintf("key%d", r.Intn(8)), v: fmt.Sprintf("val-%d-%d", i, r.Intn(1000))}
	}
	return ops
}

func model(ops []op) map[string]string {
	m := map[string]string{}
	for _, o := range ops {
		if o.del {
			delete(m, o.k)
		} else {
			m[o.k] = o.v
		}
	}
	return m
}

func dump(s *Store) map[string]string {
	m := map[string]string{}
	for k := range s.data {
		if v, ok := s.Get(k); ok {
			m[k] = string(v)
		}
	}
	return m
}

// Crash at every byte offset of the log: after recovery the state must equal
// the model of some prefix of the operations (no torn or reordered state).
func TestCrashAtEveryByteOffset(t *testing.T) {
	ops := workload(42, 40)
	var total int64
	for _, o := range ops {
		if o.del {
			total += int64(len(encode(opDel, o.k, nil, 0)))
		} else {
			total += int64(len(encode(opSet, o.k, []byte(o.v), 0)))
		}
	}
	for cut := int64(1); cut <= total; cut++ {
		dir := t.TempDir()
		s := open(t, dir, Options{CrashAfterBytes: cut})
		applied := 0
		for _, o := range ops {
			var err error
			if o.del {
				err = s.Delete(o.k)
			} else {
				err = s.Set(o.k, []byte(o.v), 0)
			}
			if err != nil {
				if !errors.Is(err, ErrCrash) {
					t.Fatal(err)
				}
				break
			}
			applied++
		}
		r := open(t, dir, Options{})
		got := dump(r)
		r.Close()
		// every acknowledged write must be present; the in-flight one may not be
		if !reflect.DeepEqual(got, model(ops[:applied])) {
			t.Fatalf("cut=%d applied=%d\n got  %v\n want %v", cut, applied, got, model(ops[:applied]))
		}
	}
}

func TestCrashDuringSnapshot(t *testing.T) {
	for _, point := range []string{"snap-written", "snap-renamed", "log-truncated"} {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			ops := workload(7, 30)
			s := open(t, dir, Options{Hook: func(p string) error {
				if p == point {
					return errors.New("boom")
				}
				return nil
			}})
			for _, o := range ops {
				if o.del {
					s.Delete(o.k)
				} else {
					s.Set(o.k, []byte(o.v), 0)
				}
			}
			if err := s.Snapshot(); !errors.Is(err, ErrCrash) {
				t.Fatalf("want crash, got %v", err)
			}
			r := open(t, dir, Options{})
			defer r.Close()
			if got := dump(r); !reflect.DeepEqual(got, model(ops)) {
				t.Fatalf("got %v want %v", got, model(ops))
			}
		})
	}
}

func TestBitFlipStopsReplayAtDamage(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, Options{})
	s.Set("a", []byte("1"), 0)
	first := s.LogSize()
	s.Set("b", []byte("2"), 0)
	s.Close()
	p := dir + "/" + logName
	b := readFile(t, p)
	b[first+headerSize+2] ^= 0xff
	writeFile(t, p, b)
	r := open(t, dir, Options{})
	defer r.Close()
	if _, ok := r.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	if _, ok := r.Get("b"); ok {
		t.Fatal("corrupt record must be dropped")
	}
}
