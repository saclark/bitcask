package bitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Compact runs a log compaction job unless one is already running, waiting to
// run, or the [DB] is closing. If run, it returns true and any error
// encountered. Otherwise, it immediately returns false and the error is nil.
//
// Unless [Config.CompactManually] is true, log compaction already runs
// automatically in the background when the active segment reaches the
// maximum size given by [Config.MaxSegmentSize]. However, this method allows
// callers to manually trigger a log compaction at will.
func (db *DB) Compact() (bool, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return false, ErrDatabaseClosed
	}
	db.mu.RUnlock()

	c := make(chan error)
	ok := db.triggerLogCompaction(c)
	return ok, <-c
}

// triggerLogCompaction triggers a log compaction job in the background unless
// one is already running, waiting to run, or the [DB] is closing. It returns
// true if a job was triggered, otherwise false.
//
// The result of the triggered job is sent on c. Callers who do not intend to
// recieve the result may pass nil.
func (db *DB) triggerLogCompaction(c chan error) bool {
	select {
	case db.compactChan <- c:
		return true
	default:
		return false
	}
}

func (db *DB) serveLogCompaction() {
	for {
		select {
		case c := <-db.compactChan:
			err := db.compact()
			if err != nil {
				db.emit(&LogCompactionError{err: fmt.Errorf("log compaction: failed: %w", err)})
			}
			db.emit("log compaction: succeeded")
			if c != nil {
				c <- err
			}
		case <-db.closeChan:
			return
		}
	}
}

func (db *DB) compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.emit("log compaction: started")

	if db.closed {
		return nil
	}

	var firstNewCompactedSegmentID segmentID
	activeSegmentID := db.fwID

	var sid segmentID
	var fw *os.File
	var offset int64
	for k, v := range db.kvIndex {
		// Skip any values persisted to the active segment or later at the time
		// this log compaction was kicked off.
		if v.SegmentID >= activeSegmentID {
			continue
		}

		vfr, ok := db.frIndex[v.SegmentID]
		if !ok {
			continue
		}

		headerAndKeySize := headerSize + int64(len(k))
		recordSize := headerAndKeySize + int64(v.Size)

		// Create a new segment if necessary.
		if fw == nil || offset+recordSize > db.cfg.MaxSegmentSize {
			sid = db.nextCompactedSegmentID()
			fn := sid.Filename()

			if fw != nil {
				if err := syncAndClose(fw); err != nil {
					return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
				}
			} else {
				firstNewCompactedSegmentID = sid
			}

			var err error
			fw, err = os.OpenFile(filepath.Join(db.dir, fn), segFileFlag, segFileMode)
			if err != nil {
				return fmt.Errorf("opening new compacted segment file for writing: %v", err)
			}

			fr, err := os.Open(filepath.Join(db.dir, fn))
			if err != nil {
				_ = fw.Close() // ignore error, nothing was written to it
				return fmt.Errorf("opening new compacted segment file for reading: %v", err)
			}

			db.frIndex[sid] = fr
			offset = 0
		}

		// Copy the record to the new compacted segment.
		r := io.NewSectionReader(vfr, v.Offset-headerAndKeySize, recordSize)
		written, err := io.Copy(fw, r)
		if err != nil {
			_ = syncAndClose(fw) // TODO: Don't ignore this error if it was a Sync error.
			return fmt.Errorf("copying record to new compacted segment file: %v", err)
		}

		offset += written

		// Update the index.
		v.SegmentID = sid
		v.Offset = offset - int64(v.Size)
		db.kvIndex[k] = v
	}

	if fw != nil {
		if err := syncAndClose(fw); err != nil {
			return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
		}
	}

	for sid, fr := range db.frIndex {
		if sid < firstNewCompactedSegmentID || (!sid.Compacted() && sid < activeSegmentID) {
			delete(db.frIndex, sid)
			_ = fr.Close()           // ignore error, read-only file
			_ = os.Remove(fr.Name()) // ignore error
		}
	}

	return nil
}
