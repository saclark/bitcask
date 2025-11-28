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
func (db *DB) CompactLog() (started bool, err error) {
	return new(logCompactor).compactLog(db)
}

type managedFile struct {
	f         *os.File
	dirty     bool
	synced    bool
	closed    bool
	cleanedUp bool
}

func newManagedFile(f *os.File) *managedFile {
	return &managedFile{f: f}
}

func (m *managedFile) Name() string {
	return m.f.Name()
}

func (m *managedFile) Read(p []byte) (n int, err error) {
	return m.f.Read(p)
}

func (m *managedFile) Write(p []byte) (n int, err error) {
	n, err = m.f.Write(p)
	if !m.dirty && n > 0 {
		m.dirty = true
	}
	return n, err
}

func (m *managedFile) Cleanup() error {
	if m == nil || m.cleanedUp {
		return nil
	}
	m.cleanedUp = true
	syncErr := m.sync()
	closeErr := m.close()
	if syncErr != nil {
		return syncErr
	}
	if closeErr != nil {
		return closeErr
	}
	return nil
}

func (m *managedFile) sync() error {
	if m == nil || m.f == nil || !m.dirty {
		return nil
	}
	if err := m.f.Sync(); err != nil {
		return &syncError{
			err: fmt.Errorf("syncing %s: %v", m.f.Name(), err),
		}
	}
	m.synced = true
	return nil
}

func (m *managedFile) close() error {
	if m == nil || m.f == nil || m.closed {
		return nil
	}
	if err := m.f.Close(); err != nil {
		return fmt.Errorf("closing %s: %v", m.f.Name(), err)
	}
	m.closed = true
	return nil
}

type logCompactor struct {
	dst                  *managedFile
	idx                  *managedFile
	dstSID               segmentID
	dstOffset            int64
	walEncoder           *walRecordEncoder
	idxEncoder           *indexRecordEncoder
	firstNewCompactedSID segmentID
}

func (c *logCompactor) compactLog(db *DB) (started bool, err error) {
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

	// Cleanup resources when complete.
	defer func() {
		dstErr := c.dst.Cleanup()
		idxErr := c.idx.Cleanup()
		dirErr := db.dir.Sync()
		if dirErr != nil {
			dirErr = &syncError{
				err: fmt.Errorf("syncing %s: %w", db.dir.Name(), dirErr),
			}
		}

		err = errors.Join(err, dstErr, idxErr, dirErr)

		// TODO: Don't emit LogCompactionError if only error was related to closing files?
		if err != nil {
			db.emit(&LogCompactionError{
				err: fmt.Errorf("log compaction: failed: %w", err),
			})
		} else {
			db.emit("log compaction: succeeded")
		}
	}()

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

	// Compact segments.
	for _, sid := range sids {
		if err = c.compactSegment(db, sid); err != nil {
			return true, fmt.Errorf(
				"compacting segment file %s: %v",
				sid.Filename(),
				err,
			)
		}
	}

	// Remove the segments we just compacted.
	db.mu.Lock()
	for sid, fr := range db.frs {
		if sid < c.firstNewCompactedSID ||
			(!sid.Compacted() && sid < activeSIDAsOfStart) {
			delete(db.frs, sid)
			_ = fr.Close()           // ignore error, read-only file
			_ = os.Remove(fr.Name()) // ignore error
			_ = os.Remove(           // ignore error
				strings.TrimSuffix(fr.Name(), segFileExt) + idxFileExt,
			)
		}
	}
	db.mu.Unlock()

	return true, nil
}

// TODO: Flush encoder buffers upon return?
func (c *logCompactor) compactSegment(db *DB, srcSID segmentID) error {
	srcPath := filepath.Join(db.dir.Name(), srcSID.Filename())
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening %s: %v", srcPath, err)
	}
	defer src.Close() // ignore error, read-only file

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
		origLoc, ok := db.index.Get(key)
		if !ok || origLoc.SegmentID != srcSID || origLoc.Offset != srcOffset {
			db.mu.RUnlock()
			srcOffset += nRead
			continue
		}
		db.mu.RUnlock()

		// Create a new segment if necessary.
		if c.dst == nil || c.dstOffset+rec.Size() > db.cfg.MaxSegmentSize {
			if err := c.rotateCompactedSegment(db); err != nil {
				return fmt.Errorf("rotating compacted segment: %v", err)
			}
		}

		// Encode the record.
		nWritten, err := c.walEncoder.Encode(rec)
		if err != nil {
			if nWritten == 0 || nWritten == rec.Size() {
				return err
			}
			if nWritten < 0 || nWritten > rec.Size() {
				panic("number of bytes encoded is not >= 0 and <= record size")
			}

			// Partial write, so something is borked. Not ideal, but we'll
			// prevent further writes since the segment file is now in an
			// invalid state that requires manual intervention to repair. Not
			// doing so risks further corruption or data loss.
			// TODO: Instead attempt to abandon/cleanup compaction?
			db.writeClosed = fmt.Errorf(
				"%w: preventing further writes due to invalid record: record starting at byte %d in segment file %s: %w",
				ErrDatabaseReadOnly,
				c.dstOffset,
				c.dstSID.Filename(),
				ErrPartialWrite,
			)

			return fmt.Errorf(
				"writing record starting at byte %d in segment file %s: %w: %w",
				c.dstOffset,
				c.dstSID.Filename(),
				ErrPartialWrite,
				err,
			)
		}

		// Encode the index record.
		if c.idx != nil {
			_, err := c.idxEncoder.Encode(indexRecord{
				expiry: rec.Expiry,
				offset: uint64(c.dstOffset), // safe cast, always positive
				size:   uint64(rec.Size()),  // safe cast, always positive
				Key:    rec.Key,
			})
			// If there's an error, abort creating an index file for this
			// segment and carry on.
			if err != nil {
				if err := os.Remove(c.idx.Name()); err != nil {
					return fmt.Errorf("removing %s: %w", c.idx.Name(), err)
				}
				c.idx = nil
			}
		}

		// Update the index entry if unchanged since we last looked it up.
		db.mu.Lock()
		v, ok := db.index.Get(key)
		if ok && v.SegmentID == origLoc.SegmentID && v.Offset == origLoc.Offset {
			db.index.Put(key, RecordDescription{
				SegmentID: c.dstSID,
				Offset:    c.dstOffset,
				Size:      origLoc.Size,
			})
		}
		db.mu.Unlock()

		srcOffset += nRead
		c.dstOffset += nWritten
	}

	return nil
}

func (c *logCompactor) rotateCompactedSegment(db *DB) error {
	db.mu.RLock()
	c.dstSID = db.nextCompactedSegmentID()
	db.mu.RUnlock()

	if c.dst == nil {
		c.firstNewCompactedSID = c.dstSID
	}

	dstErr := c.dst.Cleanup()
	idxErr := c.idx.Cleanup()
	if dstErr != nil || idxErr != nil {
		return errors.Join(dstErr, idxErr)
	}

	dstF, err := os.OpenFile(
		filepath.Join(db.dir.Name(), c.dstSID.Filename()),
		segFileFlag,
		segFileMode,
	)
	if err != nil {
		return fmt.Errorf(
			"opening new compacted segment file for writing: %v",
			err,
		)
	}
	c.dst = newManagedFile(dstF)
	c.walEncoder = newWALRecordEncoder(c.dst)

	idxF, err := os.OpenFile(
		filepath.Join(db.dir.Name(), c.dstSID.IndexFilename()),
		idxFileFlag,
		idxFileMode,
	)
	if err != nil {
		return fmt.Errorf(
			"opening new index file for writing: %v",
			err,
		)
	}
	c.idx = newManagedFile(idxF)
	c.idxEncoder = newIndexRecordEncoder(c.idx)

	dstROnly, err := os.Open(filepath.Join(
		db.dir.Name(),
		c.dstSID.Filename(),
	))
	if err != nil {
		return fmt.Errorf(
			"opening new compacted segment file for reading: %v",
			err,
		)
	}

	db.mu.Lock()
	db.frs[c.dstSID] = dstROnly
	db.mu.Unlock()

	c.dstOffset = 0

	return nil
}
