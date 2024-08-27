package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// LogCompactionError signifies that log compaction encountered an error. While
// this means that a full compaction of the log may not have completed, the DB
// should still be in a consistent state.
type LogCompactionError struct {
	err error
}

func (e *LogCompactionError) Error() string {
	return fmt.Sprintf("log compaction: %v", e.err)
}

func (e *LogCompactionError) Unwrap() error {
	return e.err
}

// CompactLog runs log compaction unless already underway or the [DB] is closed.
// If a log compaction process is already underway, it returns false and a nil
// error. If the [DB] is closed, it returns false and [ErrDatabaseClosed].
// Otherwise, it returns true and any error encountered, blocking until the log
// compaction process completes.
//
// Unless [Config.DisableAutomaticLogCompaction] is true, log compaction already
// runs automatically in the background when the active segment reaches the
// maximum size given by [Config.MaxSegmentSize]. However, this method allows
// callers to manually trigger a log compaction at will.
func (db *DB) CompactLog() (bool, error) {
	db.mu.RLock()
	if db.closed != nil {
		defer db.mu.RUnlock()
		return false, db.closed
	}
	db.mu.RUnlock()

	c := make(chan error)
	if ok := db.triggerLogCompaction(c); !ok {
		return false, nil
	}
	return true, <-c
}

// triggerLogCompaction triggers a log compaction job in the background unless
// one is already running. It returns true if a job was triggered, otherwise
// false.
//
// The result of the triggered job is sent on c. Callers who do not intend to
// recieve the result may pass nil.
//
// FIXME: Coordinate log compaction process with potential call to db.Close().
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
			db.emit("log compaction: started")

			err := db.compactLog()
			switch {
			case errors.Is(err, ErrDatabaseClosed):
				db.emit(fmt.Sprintf("log compaction: skipped: %v", ErrDatabaseClosed))
			case err != nil:
				db.emit(&LogCompactionError{err: fmt.Errorf("log compaction: failed: %w", err)})
			default:
				db.emit("log compaction: succeeded")
			}

			if c != nil {
				c <- err
			}
		case <-db.closeChan:
			return
		}
	}
}

func (db *DB) compactLog() error {
	db.mu.RLock()
	if db.closed != nil {
		db.mu.RUnlock()
		return db.closed
	}

	activeSID := db.fwID
	var sids []segmentID
	for sid := range db.frs {
		if sid < activeSID {
			sids = append(sids, sid)
		}
	}
	db.mu.RUnlock()

	sort.Slice(sids, func(i, j int) bool {
		return sids[i] < sids[j]
	})

	var fw *os.File
	var fwEnc *walRecordEncoder
	var wSID segmentID
	var firstNewCompactedSID segmentID
	var fwOffset int64
	for _, rSID := range sids {
		fr, err := os.Open(filepath.Join(db.dir, rSID.Filename()))
		if err != nil {
			_ = fw.Close() // ignore error, nothing was written to it
			return fmt.Errorf("opening segment file for reading: %v", err)
		}

		dec := newWALRecordDecoder(fr)
		var frOffset int64
		for {
			// Decode the record.
			var rec walRecord
			rn, err := dec.Decode(&rec)
			if err != nil {
				_ = fr.Close() // ignore error, read-only file

				if errors.Is(err, io.EOF) {
					break
				}

				// Either there was a partial write or the segment file was
				// corrupted.
				return fmt.Errorf(
					"decoding record starting at byte %d in segment file %s: %w",
					frOffset,
					fr.Name(),
					err,
				)
			}

			// Skip if expired.
			if ttl, _ := rec.TTL(); ttl <= 0 {
				frOffset += rn
				continue
			}

			// Skip if not latest version.
			key := string(rec.Key)
			db.mu.RLock()
			loc, ok := db.index[key]
			if !ok || loc.SegmentID != rSID || loc.Offset != frOffset {
				db.mu.RUnlock()
				frOffset += rn
				continue
			}
			db.mu.RUnlock()

			// Create a new segment if necessary.
			if fw == nil || fwEnc == nil || fwOffset+rec.Size() > db.cfg.MaxSegmentSize {
				db.mu.RLock()
				wSID = db.nextCompactedSegmentID()
				db.mu.RUnlock()

				if fw != nil {
					if err := syncAndClose(fw); err != nil {
						return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
					}
				} else {
					firstNewCompactedSID = wSID
				}

				var err error
				fw, err = os.OpenFile(filepath.Join(db.dir, wSID.Filename()), segFileFlag, segFileMode)
				if err != nil {
					return fmt.Errorf("opening new compacted segment file for writing: %v", err)
				}

				fwEnc = newWALRecordEncoder(fw)

				fr, err := os.Open(filepath.Join(db.dir, wSID.Filename()))
				if err != nil {
					_ = fw.Close() // ignore error, nothing was written to it
					return fmt.Errorf("opening new compacted segment file for reading: %v", err)
				}

				db.mu.Lock()
				db.frs[wSID] = fr
				db.mu.Unlock()

				fwOffset = 0
			}

			// Encode the record.
			wn, err := fwEnc.Encode(rec)
			if err != nil {
				if wn <= 0 || wn >= rec.Size() {
					return err
				}

				// Partial write, so something is borked. Not ideal, but we'll prevent
				// further writes since the segment file is now in an invalid state that
				// requires manual intervention to repair. Not doing so risks further
				// corruption or data loss.
				db.writeClosed = fmt.Errorf(
					"%w: preventing further writes due to invalid record: record starting at byte %d in segment file %s: %w",
					ErrDatabaseReadOnly,
					fwOffset,
					wSID.Filename(),
					ErrPartialWrite,
				)

				return fmt.Errorf(
					"writing record starting at byte %d in segment file %s: %w: %w",
					fwOffset,
					wSID.Filename(),
					ErrPartialWrite,
					err,
				)
			}

			// Update the index.
			loc.SegmentID = wSID
			loc.Offset = fwOffset

			db.mu.Lock()
			db.index[key] = loc
			db.mu.Unlock()

			frOffset += rn
			fwOffset += wn
		}
	}

	if fw != nil {
		if err := syncAndClose(fw); err != nil {
			return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
		}
	}

	db.mu.Lock()
	for sid, fr := range db.frs {
		if sid < firstNewCompactedSID || (!sid.Compacted() && sid < activeSID) {
			delete(db.frs, sid)
			_ = fr.Close()           // ignore error, read-only file
			_ = os.Remove(fr.Name()) // ignore error
		}
	}
	db.mu.Unlock()

	return nil
}
