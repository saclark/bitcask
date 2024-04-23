package bitcask

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLogCompaction_AllEligibleDataOutOfDate_OnlyActiveDataFileOrLaterRemains(t *testing.T) {
	c := make(chan any)
	config := DefaultConfig()
	config.MaxKeySize = 4
	config.MaxValueSize = 4
	config.MaxFileSize = 52
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

	path, db, err := openTmpDB(t, "TestCompact_ResultsInEmptyDataFile", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	assertOp(t, db, "Put", "aaaa", []byte("0000"), nil) // Data file = 28 bytes, index = {"aaaa"}
	assertOp(t, db, "Del", "aaaa", nil, nil)            // Data file = 52 bytes, index = {}
	assertOp(t, db, "Put", "bbbb", []byte("1111"), nil) // Triggers switchover and compaction of previous data file.

	<-c // wait for the log compaction to begin so Close does not preempt it.
	if err := db.Close(); err != nil {
		t.Fatalf("closing DB: %v", err)
	}

	dfs, err := filepath.Glob(filepath.Join(path, "*.data"))
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	if want := 1; len(dfs) != want {
		t.Fatalf("want %d files, got %d", want, len(dfs))
	}
}
