package bitcask

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLogCompaction_AllEligibleDataOutOfDate_OnlyActiveSegmentOrLaterRemains(t *testing.T) {
	c := make(chan any)
	config := DefaultConfig()
	config.MaxKeySize = 4
	config.MaxValueSize = 4
	config.MaxSegmentSize = 52
	config.HandleEvent = func(event any) {
		switch ev := event.(type) {
		case string:
			if ev == "log compaction: started" {
				c <- ev
			}
		default:
			t.Fatalf("unexpected event: %v", event)
		}
	}

	path, db, err := openTmpDB(t, "TestLogCompaction_AllEligibleDataOutOfDate_OnlyActiveSegmentOrLaterRemains", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	assertOp(t, db, "Put", "aaaa", []byte("0000"), 0, nil) // Segment = 28 bytes, index = {"aaaa"}
	assertOp(t, db, "Del", "aaaa", nil, 0, nil)            // Segment = 52 bytes, index = {}
	assertOp(t, db, "Put", "bbbb", []byte("1111"), 0, nil) // Triggers segment rotation and compaction of previous segment.
	assertOp(t, db, "Put", "c", []byte("2"), 1*time.Second, nil)

	v, err := db.Get("c")
	if err != nil {
		t.Fatalf("getting value: %v", err)
	}
	if want := []byte("2"); !bytes.Equal(v, want) {
		t.Fatalf("want %s, got %s", want, v)
	}

	time.Sleep(1500 * time.Millisecond) // let key "c" expire

	v, err = db.Get("c")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("getting value: %v", err)
	}
	if v != nil {
		t.Fatalf("want nil, got %s", v)
	}

	v, err = db.Get("aaaa")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("getting value: %v", err)
	}
	if v != nil {
		t.Fatalf("want nil, got %s", v)
	}

	v, err = db.Get("bbbb")
	if err != nil {
		t.Fatalf("getting value: %v", err)
	}
	if want := []byte("1111"); !bytes.Equal(v, want) {
		t.Fatalf("want %s, got %s", want, v)
	}

	<-c // Ensure we aren't front running log compaction.
	if err := db.Close(); err != nil {
		t.Fatalf("closing DB: %v", err)
	}

	segs, err := filepath.Glob(filepath.Join(path, "*.seg"))
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	if want := 1; len(segs) != want {
		t.Fatalf("want %d files, got %d", want, len(segs))
	}

	db, err = Open(path, DefaultConfig())
	if err != nil {
		t.Fatalf("re-opening DB: %v", err)
	}

	v, err = db.Get("aaaa")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("getting value: %v", err)
	}
	if v != nil {
		t.Fatalf("want nil, got %s", v)
	}

	v, err = db.Get("bbbb")
	if err != nil {
		t.Fatalf("getting value: %v", err)
	}
	if want := []byte("1111"); !bytes.Equal(v, want) {
		t.Fatalf("want %s, got %s", want, v)
	}

	v, err = db.Get("c")
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("getting value: %v", err)
	}
	if v != nil {
		t.Fatalf("want nil, got %s", v)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("closing DB: %v", err)
	}
}
