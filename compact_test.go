package bitcask

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

func TestLogCompaction_AllEligibleDataOutOfDate_OnlyActiveSegmentOrLaterRemains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var compactionStarted bool
		config := DefaultConfig()
		config.MaxKeySize = 4
		config.MaxValueSize = 4
		config.MaxSegmentSize = 52
		config.HandleEvent = func(event any) {
			switch ev := event.(type) {
			case string:
				if ev == "log compaction: started" {
					compactionStarted = true
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

		// Segment = 28 bytes, index = {"aaaa"}
		assertOp(t, db, "Put", "aaaa", []byte("0000"), 0, nil)

		// Segment = 52 bytes, index = {}
		assertOp(t, db, "Del", "aaaa", nil, 0, nil)

		synctest.Wait()
		if compactionStarted {
			t.Fatalf("compaction sooner than expected")
		}

		// Triggers segment rotation and compaction of previous segment.
		assertOp(t, db, "Put", "bbbb", []byte("1111"), 0, nil)

		// Wait for log compaction to start.
		synctest.Wait()
		if !compactionStarted {
			t.Fatalf("compaction did not trigger when expected")
		}

		// Wait for log compaction to complete.
		synctest.Wait()

		assertOp(t, db, "PutWithTTL", "c", []byte("2"), time.Second, nil)

		// Check "c" just before it expires.
		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		v, err := db.Get("c")
		if err != nil {
			t.Fatalf("getting value: %v", err)
		}
		if want := []byte("2"); !bytes.Equal(v, want) {
			t.Fatalf("want %s, got %s", want, v)
		}

		// Check "c" the moment it expires.
		time.Sleep(time.Second)
		synctest.Wait()
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
	})
}
