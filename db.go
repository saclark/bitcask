// Package bitcask implements a high-performance key-value store utilizing an
// on-disk write-ahead log (WAL) for persistence, à la the [Bitcask] paper from
// Riak.
//
// [Bitcask]: https://riak.com/assets/bitcask-intro.pdf
package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrKeyTooLarge      = errors.New("key exceeds configured maximum size")
	ErrValueTooLarge    = errors.New("value exceeds configured maximum size")
	ErrKeyNotFound      = errors.New("key not found")
	ErrPartialWrite     = errors.New("partial write")
	ErrDatabaseReadOnly = errors.New("database read-only")
	ErrDatabaseLocked   = errors.New("database locked")
	ErrDatabaseClosed   = errors.New("database closed")
)

// rwLocker defines the interface for a reader-writer lock.
type rwLocker interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// stdMutex wraps a sync.Mutex in the rwLocker interface so locking strategies
// can be swapped out.
type stdMutex struct{ sync.Mutex }

func (l *stdMutex) RLock()   { l.Lock() }
func (l *stdMutex) RUnlock() { l.Unlock() }

// DB implements a high-performance, persistent key-value store. It is safe for
// concurrent use.
type DB struct {
	cfg         Config
	dir         *os.File
	emit        func(any)
	fw          *os.File               // Active segment opened for writing.
	fwID        segmentID              // Active segment ID.
	fwEncoder   *walRecordEncoder      // Active segment encoder.
	fwOffset    int64                  // Active segment current offset.
	frs         map[segmentID]*os.File // Set of segments opened for reading.
	index       RecordIndexer          // Records indexed by key.
	mu          rwLocker
	compacting  chan struct{}
	closed      error
	writeClosed error
}

// Open returns a [DB] using the directory at path to load and store data. If no
// such directory exists, it is created, along with any necessary parent
// directories.
//
// Subsequent calls to Open by this or any other process before calling
// [DB.Close] will result in an [ErrDatabaseLocked] error as only one [DB]
// instance may run against the directory given by path at any moment.
// TODO: Call os.Sync on DB directory parent dir?
func Open(path string, config Config) (*DB, error) {
	config = config.hydrated()
	if err := config.validate(); err != nil {
		return nil, err
	}

	// Create the directory if it doesn't exist.
	if err := os.MkdirAll(path, dbDirMode); err != nil {
		return nil, fmt.Errorf("creating database directory: %v", err)
	}

	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening database directory: %v", err)
	}

	if err := acquireDirLock(dir); err != nil {
		return nil, err
	}

	db, err := open(dir, config)
	if err != nil {
		_ = releaseDirLock(dir)
		return nil, err
	}

	return db, nil
}

func open(dir *os.File, config Config) (*DB, error) {
	// List all segment filenames.
	segNames, err := filepath.Glob(filepath.Join(dir.Name(), "*"+segFileExt))
	if err != nil {
		return nil, fmt.Errorf("finding segment files in directory: %v", err)
	}

	// Parse the filenames as segmentIDs and sort them. The active segment has
	// the largest ID.
	var sids []segmentID
	for _, fname := range segNames {
		fname = filepath.Base(fname)
		ext := filepath.Ext(fname)
		if ext != segFileExt {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(fname, ext), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid segment filename: %s", fname)
		}
		sids = append(sids, segmentID(id))
	}

	// List all index filenames.
	idxNames, err := filepath.Glob(filepath.Join(dir.Name(), "*"+idxFileExt))
	if err != nil {
		return nil, fmt.Errorf("finding index files in directory: %v", err)
	}

	// Construct the set of segments having index files.
	indexedSegments := make(map[segmentID]struct{}, len(sids))
	for _, fname := range idxNames {
		fname = filepath.Base(fname)
		ext := filepath.Ext(fname)
		if ext != idxFileExt {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(fname, ext), 10, 64)
		if err != nil {
			continue
		}
		indexedSegments[segmentID(id)] = struct{}{}
	}

	slices.Sort(sids)

	// Open the active segment file for writing, creating a new one if none
	// exist.
	if len(sids) == 0 {
		sids = append(sids, minUncompactedSegmentID.Inc())
	}

	fwID := sids[len(sids)-1]
	fw, err := createFile(dir, fwID.Filename(), segFileFlag, segFileMode)
	if err != nil {
		return nil, fmt.Errorf("opening active segment file for writing: %v", err)
	}

	// Open and index all segment files.
	frs := make(map[segmentID]*os.File, len(sids))
	index := mapIndex(make(map[string]RecordDescription))

	for _, sid := range sids {
		fr, err := os.Open(filepath.Join(dir.Name(), sid.Filename()))
		if err != nil {
			_ = fw.Close() // ignore error, nothing was written to it
			return nil, fmt.Errorf("opening segment file for reading: %v", err)
		}

		frs[sid] = fr

		// Index the segment.
		if _, ok := indexedSegments[sid]; ok {
			idx, err := os.Open(filepath.Join(dir.Name(), sid.IndexFilename()))
			if err != nil {
				_ = fw.Close() // ignore error, nothing was written to it
				return nil, fmt.Errorf("opening index file for reading: %v", err)
			}

			dec := newIndexRecordDecoder(idx)
			var offset int64
			for {
				var rec indexRecord
				n, err := dec.Decode(&rec)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					_ = fw.Close()  // ignore error, nothing was written to it
					_ = idx.Close() // ignore error, read-only file
					for _, fr := range frs {
						_ = fr.Close() // ignore error, read-only file
					}

					// Either there was a partial write or the segment file was
					// corrupted.
					return nil, fmt.Errorf(
						"decoding index record starting at byte %d in index file %s: %w",
						offset,
						idx.Name(),
						err,
					)
				}

				if ttl, _ := rec.RecordTTL(); ttl <= 0 {
					index.Delete(string(rec.Key))
				} else {
					index.Put(string(rec.Key), RecordDescription{
						SegmentID: sid,
						Offset:    rec.RecordOffset(),
						Size:      rec.RecordSize(),
					})
				}

				offset += n
			}

			_ = idx.Close() // ignore error, read-only file
		} else {
			dec := newWALRecordDecoder(fr)
			var offset int64
			for {
				var rec walRecord
				n, err := dec.Decode(&rec)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					_ = fw.Close() // ignore error, nothing was written to it
					for _, fr := range frs {
						_ = fr.Close() // ignore error, read-only file
					}

					// Either there was a partial write or the segment file was
					// corrupted.
					return nil, fmt.Errorf(
						"decoding record starting at byte %d in segment file %s: %w",
						offset,
						fr.Name(),
						err,
					)
				}

				ttl, _ := rec.TTL()
				if len(rec.Value) == 0 || ttl <= 0 {
					index.Delete(string(rec.Key))
				} else {
					index.Put(string(rec.Key), RecordDescription{
						SegmentID: sid,
						Offset:    offset,
						Size:      rec.Size(),
					})
				}

				offset += n
			}
		}
	}

	info, err := fw.Stat()
	if err != nil {
		_ = fw.Close() // ignore error, nothing was written to it
		for _, fr := range frs {
			_ = fr.Close() // ignore error, read-only file
		}
		return nil, fmt.Errorf("statting active segment file opened for writing: %v", err)
	}

	var mu rwLocker
	if config.UseStandardMutex {
		mu = &stdMutex{}
	} else {
		mu = &sync.RWMutex{}
	}

	db := &DB{
		cfg:        config,
		dir:        dir,
		emit:       config.HandleEvent,
		fw:         fw,
		fwID:       fwID,
		fwEncoder:  newWALRecordEncoder(fw),
		fwOffset:   info.Size(),
		frs:        frs,
		index:      index,
		mu:         mu,
		compacting: make(chan struct{}, 1),
	}

	// Mark log compaction as available to run.
	db.compacting <- struct{}{}

	return db, nil
}

// Get gets the value associated with key. [ErrKeyNotFound] is returned if no
// such key exists.
func (db *DB) Get(key string) ([]byte, error) {
	rec, fr, err := db.indexGet(key)
	if err != nil {
		return nil, err
	}
	v, err := readRecordValue(fr, rec.Offset, rec.Size)
	if err != nil {
		if errors.Is(err, ErrRecordExpired) {
			db.mu.Lock()
			defer db.mu.Unlock()
			db.index.Delete(key)
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return v, nil
}

func (db *DB) indexGet(key string) (RecordDescription, io.ReaderAt, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed != nil {
		return RecordDescription{}, nil, db.closed
	}
	rec, ok := db.index.Get(key)
	if !ok {
		return RecordDescription{}, nil, ErrKeyNotFound
	}
	return rec, db.frs[rec.SegmentID], nil
}

// Put inserts or overwrites the value associated with key and does not expire.
// A nil value deletes the key-value pair.
func (db *DB) Put(key string, value []byte) error {
	if len(key) > db.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.cfg.MaxValueSize {
		return ErrValueTooLarge
	}
	return db.put(key, value, noExpiry)
}

// PutWithTTL inserts or overwrites the value and time to live (TTL) duration
// associated with key. The key-value pair expires after the ttl duration
// elapses. A nil value or TTL duration <= 0 deletes the key-value pair.
func (db *DB) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	if len(key) > db.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.cfg.MaxValueSize {
		return ErrValueTooLarge
	}
	if len(value) == 0 || ttl <= 0 {
		return db.Delete(key)
	}
	return db.put(key, value, ttlExpiry(ttl))
}

// Delete deletes the key-value pair associated with key.
func (db *DB) Delete(key string) error {
	return db.put(key, nil, noExpiry)
}

// put persists the key-value pair and updates the in-memory index. A nil value
// deletes the key-value pair.
func (db *DB) put(key string, value []byte, expiry expiryTimestamp) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.writeClosed != nil {
		return db.writeClosed
	}

	if len(value) == 0 {
		if _, ok := db.index.Get(key); !ok {
			return nil
		}
	}

	rec := newWALRecord([]byte(key), value, expiry)
	if db.fwOffset+rec.Size() > db.cfg.MaxSegmentSize {
		if err := db.rotateSegment(); err != nil {
			return fmt.Errorf("rotating active segment: %w", err)
		}
		if !db.cfg.DisableAutomaticLogCompaction {
			go db.CompactLog()
		}
	}

	n, err := db.fwEncoder.Encode(rec)
	if err != nil {
		if n <= 0 || n >= rec.Size() {
			return err
		}

		// Partial write, so something is borked. Not ideal, but we'll prevent
		// further writes since the segment file is now in an invalid state that
		// requires manual intervention to repair. Not doing so risks further
		// corruption or data loss.
		db.writeClosed = fmt.Errorf(
			"%w: preventing further writes due to invalid record: record starting at byte %d in segment file %s: %w",
			ErrDatabaseReadOnly,
			db.fwOffset,
			db.fwID.Filename(),
			ErrPartialWrite,
		)

		return fmt.Errorf(
			"writing record starting at byte %d in segment file %s: %w: %w",
			db.fwOffset,
			db.fwID.Filename(),
			ErrPartialWrite,
			err,
		)
	}

	if len(value) == 0 {
		db.index.Delete(key)
	} else {
		db.index.Put(key, RecordDescription{
			SegmentID: db.fwID,
			Offset:    db.fwOffset,
			Size:      rec.Size(),
		})
	}

	db.fwOffset += n

	return nil
}

// RotateSegment causes the database to begin writing to a new active segment,
// making the old active segment eligible for compaction.
//
// Active segment rotation already happens automatically when the active
// segment reaches the maximum size given by [Config.MaxSegmentSize]. However,
// this method allows callers to manually trigger a rotation at will.
func (db *DB) RotateSegment() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.writeClosed != nil {
		return db.writeClosed
	}
	return db.rotateSegment()
}

// rotateSegment causes the database to begin writing to a new active segment.
// Callers must take care to Lock() before calling this method.
func (db *DB) rotateSegment() (err error) {
	sid := db.nextUncompactedSegmentID()

	var fw, fr *os.File
	fw, err = createFile(db.dir, sid.Filename(), segFileFlag, segFileMode)
	if err != nil {
		return fmt.Errorf("opening new active segment file for writing: %v", err)
	}

	// Attempt to close and remove files if an error occurs.
	defer func() {
		if err != nil {
			_ = fw.Close()           // ignore error, nothing was written to it
			_ = os.Remove(fw.Name()) // ignore error
			if fr != nil {
				_ = fr.Close() // ignore error, read-only file
			}
		}
	}()

	fr, err = os.Open(fw.Name())
	if err != nil {
		return fmt.Errorf("opening new active segment file for reading: %v", err)
	}

	if err = syncAndClose(db.fw); err != nil {
		return err
	}

	db.fw = fw
	db.fwEncoder = newWALRecordEncoder(fw)
	db.fwOffset = 0
	db.fwID = sid
	db.frs[sid] = fr

	return nil
}

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

type compactionState struct {
	dst                  *managedFile
	dstSID               segmentID
	dstOffset            int64
	walEncoder           *walRecordEncoder
	idx                  *managedFile
	idxEncoder           *indexRecordEncoder
	firstNewCompactedSID segmentID
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

	s := &compactionState{}

	// Cleanup resources when complete.
	defer func() {
		dstErr := s.dst.Cleanup()
		idxErr := s.idx.Cleanup()
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

	// Compact segments.
	for _, sid := range sids {
		if err = db.compactSegment(s, sid); err != nil {
			return true, fmt.Errorf(
				"compacting segment file %s: %w",
				sid.Filename(),
				err,
			)
		}
	}

	// Remove the segments we just compacted.
	db.mu.Lock()
	for sid, fr := range db.frs {
		if sid < s.firstNewCompactedSID ||
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

func (db *DB) compactSegment(s *compactionState, srcSID segmentID) error {
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
		if s.dst == nil || s.dstOffset+rec.Size() > db.cfg.MaxSegmentSize {
			if err := db.rotateCompactedSegment(s); err != nil {
				return fmt.Errorf("rotating compacted segment: %w", err)
			}
		}

		// Encode the record.
		nWritten, err := s.walEncoder.Encode(rec)
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
				s.dstOffset,
				s.dstSID.Filename(),
				ErrPartialWrite,
			)

			return fmt.Errorf(
				"writing record starting at byte %d in segment file %s: %w: %w",
				s.dstOffset,
				s.dstSID.Filename(),
				ErrPartialWrite,
				err,
			)
		}

		// Encode the index record.
		if s.idx != nil {
			_, err := s.idxEncoder.Encode(indexRecord{
				expiry: rec.Expiry,
				offset: uint64(s.dstOffset), // Safe cast, always positive.
				size:   uint64(rec.Size()),  // Safe cast, always positive.
				Key:    rec.Key,
			})
			// If there's an error, abort creating an index file for this
			// segment and carry on.
			if err != nil {
				if err := os.Remove(s.idx.Name()); err != nil {
					return fmt.Errorf("removing %s: %w", s.idx.Name(), err)
				}
				s.idx = nil
			}
		}

		// Update the index entry if unchanged since we last looked it up.
		db.mu.Lock()
		v, ok := db.index.Get(key)
		if ok && v.SegmentID == origLoc.SegmentID && v.Offset == origLoc.Offset {
			db.index.Put(key, RecordDescription{
				SegmentID: s.dstSID,
				Offset:    s.dstOffset,
				Size:      origLoc.Size,
			})
		}
		db.mu.Unlock()

		srcOffset += nRead
		s.dstOffset += nWritten
	}

	return nil
}

func (db *DB) rotateCompactedSegment(s *compactionState) error {
	db.mu.RLock()
	s.dstSID = db.nextCompactedSegmentID()
	db.mu.RUnlock()

	if s.dst == nil {
		s.firstNewCompactedSID = s.dstSID
	}

	dstErr := s.dst.Cleanup()
	idxErr := s.idx.Cleanup()
	if dstErr != nil || idxErr != nil {
		return errors.Join(dstErr, idxErr)
	}

	dstF, err := os.OpenFile(
		filepath.Join(db.dir.Name(), s.dstSID.Filename()),
		segFileFlag,
		segFileMode,
	)
	if err != nil {
		return fmt.Errorf(
			"opening new compacted segment file for writing: %v",
			err,
		)
	}
	s.dst = newManagedFile(dstF)
	s.walEncoder = newWALRecordEncoder(s.dst)

	idxF, err := os.OpenFile(
		filepath.Join(db.dir.Name(), s.dstSID.IndexFilename()),
		idxFileFlag,
		idxFileMode,
	)
	if err != nil {
		return fmt.Errorf(
			"opening new index file for writing: %v",
			err,
		)
	}
	s.idx = newManagedFile(idxF)
	s.idxEncoder = newIndexRecordEncoder(s.idx)

	dstROnly, err := os.Open(filepath.Join(
		db.dir.Name(),
		s.dstSID.Filename(),
	))
	if err != nil {
		return fmt.Errorf(
			"opening new compacted segment file for reading: %v",
			err,
		)
	}

	db.mu.Lock()
	db.frs[s.dstSID] = dstROnly
	db.mu.Unlock()

	s.dstOffset = 0

	return nil
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

// Sync commits the current contents of the active segment to stable storage.
// Typically, this means flushing the file system's in-memory copy of recently
// written data to disk.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed != nil {
		return db.closed
	}
	return db.fw.Sync()
}

// Close closes the database. It syncs the active segment, closes all open file
// handles, and releases it's lock on the database directory. It blocks until
// log compaction has completed if one is running at the time.
//
// Once Close has been called on a [DB], it may not be reused; future calls
// to Close or other methods such as [DB.Get] or [DB.Put] will return
// [ErrDatabaseClosed].
func (db *DB) Close() error {
	// Wait for any running log compaction job to complete and prevent another
	// log compaction from running.
	if _, ok := <-db.compacting; ok {
		close(db.compacting)
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed != nil {
		return db.closed
	}

	db.closed = ErrDatabaseClosed
	db.writeClosed = ErrDatabaseClosed

	if err := syncAndClose(db.fw); err != nil {
		return err
	}

	for _, fr := range db.frs {
		_ = fr.Close() // ignore error, read-only file
	}

	if err := releaseDirLock(db.dir); err != nil {
		return fmt.Errorf("releasing directory lock: %v", err)
	}

	if err := db.dir.Close(); err != nil {
		return fmt.Errorf("closing the database directory: %v", err)
	}

	return nil
}
