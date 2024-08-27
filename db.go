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
	"sort"
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

// recordLoc specifies the location and size of a record.
type recordLoc struct {
	SegmentID segmentID // The ID of the segment in which the record is stored.
	Offset    int64     // The byte offset at which the record is stored within the segment.
	Size      int64     // The byte size of the record.
}

// DB implements a high-performance, persistent key-value store. It is safe for
// concurrent use.
type DB struct {
	cfg         Config
	dir         string
	emit        func(any)
	fw          *os.File               // Active segment opened for writing.
	fwID        segmentID              // Active segment ID.
	fwEncoder   *walRecordEncoder      // Active segment encoder.
	fwOffset    int64                  // Active segment current offset.
	frs         map[segmentID]*os.File // Set of segments opened for reading.
	index       map[string]recordLoc   // Records indexed by key.
	mu          rwLocker
	compactChan chan chan error
	closeChan   chan struct{}
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
func Open(path string, config Config) (*DB, error) {
	config = config.hydrated()
	if err := config.validate(); err != nil {
		return nil, err
	}

	// Create the directory if it doesn't exist.
	if err := os.MkdirAll(path, dbDirMode); err != nil {
		return nil, fmt.Errorf("creating directory: %v", err)
	}

	// Lock the DB.
	lockPath := filepath.Join(path, lockFilename)
	if _, err := os.OpenFile(lockPath, lockFileFlag, lockFileMode); err != nil {
		if os.IsExist(err) {
			return nil, ErrDatabaseLocked
		}
		return nil, err
	}

	// FIXME: Be sure to remove the DB lock if any errors occur below.

	// List all segment filenames.
	fns, err := filepath.Glob(filepath.Join(path, "*"+segFileExt))
	if err != nil {
		return nil, fmt.Errorf("finding segment files in directory: %v", err)
	}

	// Parse the filenames as segmentIDs and sort them. The active segment has
	// the largest ID.
	var sids []segmentID
	for _, fn := range fns {
		fn = filepath.Base(fn)
		ext := filepath.Ext(fn)
		if ext != segFileExt {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid segment filename: %s", fn)
		}
		sids = append(sids, segmentID(id))
	}

	sort.Slice(sids, func(i, j int) bool {
		return sids[i] < sids[j]
	})

	// Open the active segment file for writing, creating a new one if none
	// exist.
	if len(sids) == 0 {
		sids = append(sids, minUncompactedSegmentID.Inc())
	}

	fwID := sids[len(sids)-1]
	fw, err := os.OpenFile(
		filepath.Join(path, fwID.Filename()),
		segFileFlag,
		segFileMode,
	)
	if err != nil {
		return nil, fmt.Errorf("opening active segment file for writing: %v", err)
	}

	// Open and index all segment files.
	frs := make(map[segmentID]*os.File, len(sids))
	index := make(map[string]recordLoc)

	for _, sid := range sids {
		fr, err := os.Open(filepath.Join(path, sid.Filename()))
		if err != nil {
			_ = fw.Close() // ignore error, nothing was written to it
			return nil, fmt.Errorf("opening segment file for reading: %v", err)
		}

		frs[sid] = fr

		// Index the segment.
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

			k := string(rec.Key)

			ttl, _ := rec.TTL()
			if len(rec.Value) == 0 || ttl <= 0 {
				delete(index, k)
			} else {
				index[k] = recordLoc{
					SegmentID: sid,
					Offset:    offset,
					Size:      rec.Size(),
				}
			}

			offset += n
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
		cfg:         config,
		dir:         path,
		emit:        config.HandleEvent,
		fw:          fw,
		fwID:        fwID,
		fwEncoder:   newWALRecordEncoder(fw),
		fwOffset:    info.Size(),
		frs:         frs,
		index:       index,
		mu:          mu,
		compactChan: make(chan chan error),
		closeChan:   make(chan struct{}),
	}

	// Start listening for log compaction triggers in the background.
	go db.serveLogCompaction()

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
			delete(db.index, key)
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return v, nil
}

func (db *DB) indexGet(key string) (recordLoc, io.ReaderAt, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed != nil {
		return recordLoc{}, nil, db.closed
	}
	rec, ok := db.index[key]
	if !ok {
		return recordLoc{}, nil, ErrKeyNotFound
	}
	return rec, db.frs[rec.SegmentID], nil
}

// Put inserts or overwrites the value associated with key.
func (db *DB) Put(key string, value []byte) error {
	if len(key) > db.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.cfg.MaxValueSize {
		return ErrValueTooLarge
	}
	return db.put(key, value, 0)
}

// PutWithTTL inserts or overwrites the value associated with key along with a
// time to live (TTL) duration, after which the value is considered expired.
// Values with a TTL <= 0 are deleted.
func (db *DB) PutWithTTL(key string, value []byte, ttl time.Duration) error {
	if len(key) > db.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.cfg.MaxValueSize {
		return ErrValueTooLarge
	}
	if ttl <= 0 {
		value = nil
	}
	return db.put(key, value, ttl)
}

// Delete deletes the key-value pair associated with key.
func (db *DB) Delete(key string) error {
	return db.put(key, nil, 0)
}

// put persists the key-value pair and updates the in-memory index. A nil value
// or negative TTL deletes the key-value pair.
func (db *DB) put(key string, value []byte, ttl time.Duration) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.writeClosed != nil {
		return db.writeClosed
	}

	// Delete the record if value is nil or TTL < 0.
	if value == nil || ttl < 0 {
		if _, ok := db.index[key]; !ok {
			return nil
		}
		value, ttl = nil, 0
	}

	rec := newWALRecord([]byte(key), value, ttl)
	if db.fwOffset+rec.Size() > db.cfg.MaxSegmentSize {
		if err := db.rotateSegment(); err != nil {
			return fmt.Errorf("rotating active segment: %w", err)
		}
		if !db.cfg.DisableAutomaticLogCompaction {
			db.triggerLogCompaction(nil)
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
		delete(db.index, key)
	} else {
		db.index[key] = recordLoc{
			SegmentID: db.fwID,
			Offset:    db.fwOffset,
			Size:      rec.Size(),
		}
	}

	db.fwOffset += n

	return nil
}

// Keys iterates over all keys, passing each key to f and terminating when f
// returns false or all keys have been enumerated.
func (db *DB) EachKey(f func(key string) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed != nil {
		return db.closed
	}
	for k := range db.index {
		if !f(k) {
			return nil
		}
	}
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
	db.mu.Lock()
	if db.closed != nil {
		defer db.mu.Unlock()
		return db.closed
	}

	// Signal the background goroutine to stop. This must be done *after*
	// releasing the lock and by sending on, rather than closing, the channel in
	// order to wait for any log compaction job waiting to obtain a lock to
	// complete refore we return.
	defer func() { db.closeChan <- struct{}{} }()
	defer db.mu.Unlock()

	db.closed = ErrDatabaseClosed
	db.writeClosed = ErrDatabaseClosed

	if err := syncAndClose(db.fw); err != nil {
		return fmt.Errorf("syncing and closing active segment file opened for writing: %w", err)
	}

	for _, fr := range db.frs {
		_ = fr.Close() // ignore error, read-only file
	}

	err := os.Remove(filepath.Join(db.dir, lockFilename))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s file: %v", lockFilename, err)
	}

	return nil
}
