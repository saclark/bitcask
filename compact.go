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

	activeSIDAsOfStart := db.fwID
	var sids []segmentID
	for sid := range db.frs {
		if sid < activeSIDAsOfStart {
			sids = append(sids, sid)
		}
	}
	db.mu.RUnlock()

	sort.Slice(sids, func(i, j int) bool {
		return sids[i] < sids[j]
	})

	var dst *os.File
	var dstSID segmentID
	var dstOffset int64
	var encoder *walRecordEncoder
	var firstNewCompactedSID segmentID
	for _, srcSID := range sids {
		src, err := os.Open(filepath.Join(db.dir, srcSID.Filename()))
		if err != nil {
			if dst != nil {
				if err := syncAndClose(dst); err != nil {
					return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
				}
			}
			return fmt.Errorf("opening segment file for reading: %v", err)
		}

		decoder := newWALRecordDecoder(src)
		var srcOffset int64
		for {
			// Decode the record.
			var rec walRecord
			nRead, err := decoder.Decode(&rec)
			if err != nil {
				_ = src.Close() // ignore error, read-only file

				if errors.Is(err, io.EOF) {
					break
				}

				if dst != nil {
					if err := syncAndClose(dst); err != nil {
						return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
					}
				}

				// Either there was a partial write or the segment file was
				// corrupted.
				return fmt.Errorf(
					"decoding record starting at byte %d in segment file %s: %w",
					srcOffset,
					src.Name(),
					err,
				)
			}

			// Skip if expired.
			if ttl, _ := rec.TTL(); ttl <= 0 {
				srcOffset += nRead
				continue
			}

			// Skip if not latest version.
			key := string(rec.Key)
			db.mu.RLock()
			origLoc, ok := db.index[key]
			if !ok || origLoc.SegmentID != srcSID || origLoc.Offset != srcOffset {
				db.mu.RUnlock()
				srcOffset += nRead
				continue
			}
			db.mu.RUnlock()

			// Create a new segment if necessary.
			if dst == nil || encoder == nil || dstOffset+rec.Size() > db.cfg.MaxSegmentSize {
				db.mu.RLock()
				dstSID = db.nextCompactedSegmentID()
				db.mu.RUnlock()

				if dst != nil {
					if err := syncAndClose(dst); err != nil {
						return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
					}
				} else {
					firstNewCompactedSID = dstSID
				}

				var err error
				dst, err = os.OpenFile(filepath.Join(db.dir, dstSID.Filename()), segFileFlag, segFileMode)
				if err != nil {
					return fmt.Errorf("opening new compacted segment file for writing: %v", err)
				}

				encoder = newWALRecordEncoder(dst)

				dstROnly, err := os.Open(filepath.Join(db.dir, dstSID.Filename()))
				if err != nil {
					_ = dst.Close() // ignore error, nothing was written to it
					return fmt.Errorf("opening new compacted segment file for reading: %v", err)
				}

				db.mu.Lock()
				db.frs[dstSID] = dstROnly
				db.mu.Unlock()

				dstOffset = 0
			}

			// Encode the record.
			nWritten, err := encoder.Encode(rec)
			if err != nil {
				if dst != nil {
					if err := syncAndClose(dst); err != nil {
						return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
					}
				}

				if nWritten <= 0 || nWritten >= rec.Size() {
					return err
				}

				// Partial write, so something is borked. Not ideal, but we'll prevent
				// further writes since the segment file is now in an invalid state that
				// requires manual intervention to repair. Not doing so risks further
				// corruption or data loss.
				db.writeClosed = fmt.Errorf(
					"%w: preventing further writes due to invalid record: record starting at byte %d in segment file %s: %w",
					ErrDatabaseReadOnly,
					dstOffset,
					dstSID.Filename(),
					ErrPartialWrite,
				)

				return fmt.Errorf(
					"writing record starting at byte %d in segment file %s: %w: %w",
					dstOffset,
					dstSID.Filename(),
					ErrPartialWrite,
					err,
				)
			}

			// Update the index so long as it hasn't changed since we last looked it up.
			db.mu.Lock()
			v, ok := db.index[key]
			if ok && v.SegmentID == origLoc.SegmentID && v.Offset == origLoc.Offset {
				db.index[key] = recordLoc{
					SegmentID: dstSID,
					Offset:    dstOffset,
					Size:      origLoc.Size,
				}
			}
			db.mu.Unlock()

			srcOffset += nRead
			dstOffset += nWritten
		}
	}

	if dst != nil {
		if err := syncAndClose(dst); err != nil {
			return fmt.Errorf("syncing and closing compacted segment file opened for writing: %v", err)
		}
	}

	db.mu.Lock()
	for sid, fr := range db.frs {
		if sid < firstNewCompactedSID || (!sid.Compacted() && sid < activeSIDAsOfStart) {
			delete(db.frs, sid)
			_ = fr.Close()           // ignore error, read-only file
			_ = os.Remove(fr.Name()) // ignore error
		}
	}
	db.mu.Unlock()

	return nil
}
