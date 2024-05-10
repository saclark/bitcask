package bitcask

import (
	"os"
	"path/filepath"
	"testing"
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

	assertOp(t, db, "Put", "aaaa", []byte("0000"), nil) // Segment = 28 bytes, index = {"aaaa"}
	assertOp(t, db, "Del", "aaaa", nil, nil)            // Segment = 52 bytes, index = {}
	assertOp(t, db, "Put", "bbbb", []byte("1111"), nil) // Triggers switchover and compaction of previous segment.

	<-c // wait for the log compaction to begin so Close does not preempt it.
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
}
