package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
//
// TODO: The error handling here is a mess. Clean it up.
// TODO: Operate on segment and index files in parallel, where possible.
func (db *DB) CompactLog() (bool, error) {
	select {
	case _, ok := <-db.compacting:
		if !ok {
			// Database is closed.
			db.mu.RLock()
			defer db.mu.RUnlock()
			return false, db.closed
		}

		// Prevent simultaneous compactions.
		db.emit("log compaction: started")
		defer func() {
			db.compacting <- struct{}{}
		}()
	default:
		// Compaction is already underway.
		return false, nil
	}

	// Identify inactive segments to compact.
	db.mu.RLock()
	activeSIDAsOfStart := db.fwID
	var sids []segmentID
	for sid := range db.frs {
		if sid < activeSIDAsOfStart {
			sids = append(sids, sid)
		}
	}
	db.mu.RUnlock()

	slices.Sort(sids)

	var dst *os.File
	var idx *os.File
	var dstSID segmentID
	var dstOffset int64
	var walEncoder *walRecordEncoder
	var idxEncoder *indexRecordEncoder
	var firstNewCompactedSID segmentID
	for _, srcSID := range sids {
		src, err := os.Open(filepath.Join(db.dir.Name(), srcSID.Filename()))
		if err != nil {
			if err := syncAndClose(dst); err != nil {
				_ = syncAndClose(idx) // ignore error
				return true, db.compactionFailed(fmt.Errorf(
					"syncing and closing compacted segment file opened for writing: %v",
					err,
				))
			}
			if err := syncAndClose(idx); err != nil {
				return true, db.compactionFailed(fmt.Errorf(
					"syncing and closing index file opened for writing: %v",
					err,
				))
			}
			return true, db.compactionFailed(fmt.Errorf("opening segment file for reading: %v", err))
		}

		walDecoder := newWALRecordDecoder(src)
		var srcOffset int64
		for {
			// Decode the record.
			var rec walRecord
			nRead, err := walDecoder.Decode(&rec)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				_ = src.Close() // ignore error, read-only file
				if err := syncAndClose(dst); err != nil {
					_ = syncAndClose(idx) // ignore error
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing compacted segment file opened for writing: %v",
						err,
					))
				}
				if err := syncAndClose(idx); err != nil {
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing index file opened for writing: %v",
						err,
					))
				}

				// Either there was a partial write or the segment file was
				// corrupted.
				return true, db.compactionFailed(fmt.Errorf(
					"decoding record starting at byte %d in segment file %s: %w",
					srcOffset,
					src.Name(),
					err,
				))
			}

			// Skip if expired.
			if ttl, _ := rec.TTL(); ttl <= 0 {
				srcOffset += nRead
				continue
			}

			// Skip if not latest version.
			key := string(rec.Key)
			db.mu.RLock()
			origLoc, ok := db.index.Get(key)
			if !ok || origLoc.SegmentID != srcSID || origLoc.Offset != srcOffset {
				db.mu.RUnlock()
				srcOffset += nRead
				continue
			}
			db.mu.RUnlock()

			// Create a new segment if necessary.
			if dst == nil || dstOffset+rec.Size() > db.cfg.MaxSegmentSize {
				db.mu.RLock()
				dstSID = db.nextCompactedSegmentID()
				db.mu.RUnlock()

				if dst == nil {
					firstNewCompactedSID = dstSID
				}

				if err := syncAndClose(dst); err != nil {
					_ = src.Close()       // ignore error, read-only file
					_ = syncAndClose(idx) // ignore error
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing compacted segment file opened for writing: %v",
						err,
					))
				}
				if err := syncAndClose(idx); err != nil {
					_ = src.Close() // ignore error, read-only file
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing index file opened for writing: %v",
						err,
					))
				}

				var err error
				dst, err = createFile(db.dir, dstSID.Filename(), segFileFlag, segFileMode)
				if err != nil {
					_ = src.Close() // ignore error, read-only file
					return true, db.compactionFailed(fmt.Errorf(
						"opening new compacted segment file for writing: %v",
						err,
					))
				}
				idx, err = createFile(db.dir, dstSID.IndexFilename(), idxFileFlag, idxFileMode)
				if err != nil {
					_ = src.Close() // ignore error, read-only file
					_ = dst.Close() // ignore error, nothing was written to it
					return true, db.compactionFailed(fmt.Errorf(
						"opening new index file for writing: %v",
						err,
					))
				}

				walEncoder = newWALRecordEncoder(dst)
				idxEncoder = newIndexRecordEncoder(idx)

				dstROnly, err := os.Open(filepath.Join(db.dir.Name(), dstSID.Filename()))
				if err != nil {
					_ = src.Close() // ignore error, read-only file
					_ = dst.Close() // ignore error, nothing was written to it
					_ = idx.Close() // ignore error, nothing was written to it
					return true, db.compactionFailed(fmt.Errorf(
						"opening new compacted segment file for reading: %v",
						err,
					))
				}

				db.mu.Lock()
				db.frs[dstSID] = dstROnly
				db.mu.Unlock()

				dstOffset = 0
			}

			// Encode the record.
			nWritten, err := walEncoder.Encode(rec)
			if err != nil {
				_ = src.Close() // ignore error, read-only file
				if err := syncAndClose(dst); err != nil {
					_ = syncAndClose(idx) // ignore error
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing compacted segment file opened for writing: %v",
						err,
					))
				}
				if err := syncAndClose(idx); err != nil {
					return true, db.compactionFailed(fmt.Errorf(
						"syncing and closing index file opened for writing: %v",
						err,
					))
				}

				if nWritten <= 0 || nWritten >= rec.Size() {
					return true, db.compactionFailed(err)
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

				return true, db.compactionFailed(fmt.Errorf(
					"writing record starting at byte %d in segment file %s: %w: %w",
					dstOffset,
					dstSID.Filename(),
					ErrPartialWrite,
					err,
				))
			}

			// Encode the index record.
			if idx != nil {
				if _, err = idxEncoder.Encode(indexRecord{
					expiry: rec.Expiry,
					// TODO: Assert the below casts are safe.
					offset: uint64(dstOffset),  // safe cast, always positive
					size:   uint64(rec.Size()), // safe cast, always positive
					Key:    rec.Key,
				}); err != nil {
					// Abort creating an index file for this segment.
					if err = removeFile(db.dir, idx.Name()); err != nil {
						_ = src.Close() // ignore error, read-only file
						if err := syncAndClose(dst); err != nil {
							_ = syncAndClose(idx) // ignore error
							return true, db.compactionFailed(fmt.Errorf(
								"syncing and closing compacted segment file opened for writing: %v",
								err,
							))
						}
						if err := syncAndClose(idx); err != nil {
							return true, db.compactionFailed(fmt.Errorf(
								"syncing and closing index file opened for writing: %v",
								err,
							))
						}
						return true, db.compactionFailed(fmt.Errorf(
							"removing index file: %w",
							err,
						))
					}
					idx = nil
				}
			}

			// Update the index so long as it hasn't changed since we last looked it up.
			db.mu.Lock()
			v, ok := db.index.Get(key)
			if ok && v.SegmentID == origLoc.SegmentID && v.Offset == origLoc.Offset {
				db.index.Put(key, RecordDescription{
					SegmentID: dstSID,
					Offset:    dstOffset,
					Size:      origLoc.Size,
				})
			}
			db.mu.Unlock()

			srcOffset += nRead
			dstOffset += nWritten
		}

		_ = src.Close() // ignore error, read-only file
	}

	if err := syncAndClose(dst); err != nil {
		_ = syncAndClose(idx) // ignore error
		return true, db.compactionFailed(fmt.Errorf(
			"syncing and closing compacted segment file opened for writing: %v",
			err,
		))
	}
	if err := syncAndClose(idx); err != nil {
		return true, db.compactionFailed(fmt.Errorf(
			"syncing and closing index file opened for writing: %v",
			err,
		))
	}

	db.mu.Lock()
	for sid, fr := range db.frs {
		if sid < firstNewCompactedSID || (!sid.Compacted() && sid < activeSIDAsOfStart) {
			delete(db.frs, sid)
			_ = fr.Close()                                                        // ignore error, read-only file
			_ = os.Remove(fr.Name())                                              // ignore error
			_ = os.Remove(strings.TrimSuffix(fr.Name(), segFileExt) + idxFileExt) // ignore error
		}
	}
	db.mu.Unlock()

	if err := db.dir.Sync(); err != nil {
		return true, fmt.Errorf(
			"syncing parent directory %s: %w",
			db.dir.Name(),
			err,
		)
	}

	db.emit("log compaction: succeeded")
	return true, nil
}

func (db *DB) compactionFailed(err error) error {
	db.emit(&LogCompactionError{err: fmt.Errorf("log compaction: failed: %w", err)})
	return err
}
