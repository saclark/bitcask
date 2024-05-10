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

// indexedValue specifies all necessary information to efficiently retrieve a
// value from a data file.
type indexedValue struct {
	FileID fileID
	Offset int64
	Size   int
}

// DB implements a high-performance, persistent key-value store. It is safe for
// concurrent use.
type DB struct {
	cfg         Config
	dir         string
	emit        func(any)
	fw          *os.File                // Active data file opened for writing.
	fwID        fileID                  // Active data file ID.
	fwEncoder   *dataRecordEncoder      // Active data file encoder.
	fwOffset    int64                   // Active data file current offset.
	frIndex     map[fileID]*os.File     // Set of data files opened for reading.
	kvIndex     map[string]indexedValue // key-value index.
	mu          rwLocker
	compactChan chan chan error
	closeChan   chan struct{}
	closed      bool
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

	// List all data file filenames.
	fns, err := filepath.Glob(filepath.Join(path, "*"+dfFileExt))
	if err != nil {
		return nil, fmt.Errorf("finding data files in directory: %v", err)
	}

	// Parse the filenames as file IDs and sort them. The active data file
	// has the largest ID.
	var fids []fileID
	for _, fn := range fns {
		fn = filepath.Base(fn)
		ext := filepath.Ext(fn)
		if ext != dfFileExt {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(fn, ext), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid data file filename: %s", fn)
		}
		fids = append(fids, fileID(id))
	}

	sort.Slice(fids, func(i, j int) bool {
		return fids[i] < fids[j]
	})

	// Open the active data file for writing, creating a new one if none exist.
	if len(fids) == 0 {
		fids = append(fids, minUncompactedFileID.Inc())
	}

	fwID := fids[len(fids)-1]
	fw, err := os.OpenFile(
		filepath.Join(path, fwID.Filename()),
		dfFileFlag,
		dfFileMode,
	)
	if err != nil {
		return nil, fmt.Errorf("opening active data file for writing: %v", err)
	}

	// Open and index all data files.
	frIndex := make(map[fileID]*os.File, len(fids))
	kvIndex := make(map[string]indexedValue)

	for _, fid := range fids {
		fr, err := os.Open(filepath.Join(path, fid.Filename()))
		if err != nil {
			_ = fw.Close() // ignore error, nothing was written to it
			return nil, fmt.Errorf("opening data file for reading: %v", err)
		}

		frIndex[fid] = fr

		// Index the data file.
		dec := newDataRecordDecoder(fr)
		var offset int64
		for {
			var rec dataRecord
			if err := dec.Decode(&rec); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				_ = fw.Close() // ignore error, nothing was written to it
				_ = fr.Close() // ignore error, read-only file

				if errors.Is(err, io.ErrUnexpectedEOF) {
					return nil, ErrTruncatedRecord
				}

				return nil, fmt.Errorf("decoding record: %w", err)
			}

			if !rec.Valid() {
				_ = fw.Close() // ignore error, nothing was written to it
				_ = fr.Close() // ignore error, read-only file
				return nil, ErrInvalidRecord
			}

			offset += rec.Size()

			k := string(rec.Key)
			v := indexedValue{
				FileID: fid,
				Offset: offset - int64(len(rec.Value)),
				Size:   len(rec.Value),
			}

			if len(rec.Value) == 0 {
				delete(kvIndex, k)
			} else {
				kvIndex[k] = v
			}
		}
	}

	info, err := fw.Stat()
	if err != nil {
		_ = fw.Close() // ignore error, nothing was written to it
		for _, fr := range frIndex {
			_ = fr.Close() // ignore error, read-only file
		}
		return nil, fmt.Errorf("statting active data file opened for writing: %v", err)
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
		fwEncoder:   newDataRecordEncoder(fw),
		fwOffset:    info.Size(),
		frIndex:     frIndex,
		kvIndex:     kvIndex,
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
	v, fr, err := db.indexGet(key)
	if err != nil {
		return nil, err
	}

	b := make([]byte, v.Size)
	if _, err := fr.ReadAt(b, v.Offset); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, ErrTruncatedRecord
		}
		return nil, err
	}

	return b, nil
}

func (db *DB) indexGet(key string) (indexedValue, io.ReaderAt, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return indexedValue{}, nil, ErrDatabaseClosed
	}
	v, ok := db.kvIndex[key]
	if !ok {
		return indexedValue{}, nil, ErrKeyNotFound
	}
	return v, db.frIndex[v.FileID], nil
}

// Put inserts or overwrites the value associated with key.
func (db *DB) Put(key string, value []byte) error {
	if len(key) > db.cfg.MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > db.cfg.MaxValueSize {
		return ErrValueTooLarge
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrDatabaseClosed
	}

	return db.put(key, value)
}

// put persists the key-value pair and updates the in-memory index. Callers must
// take care to Lock() before calling this method.
func (db *DB) put(key string, value []byte) error {
	rec := newDataRecord([]byte(key), value)
	if db.fwOffset+rec.Size() > db.cfg.MaxFileSize && !db.cfg.ManualCompactionOnly {
		if err := db.switchover(); err != nil {
			return fmt.Errorf("switching over to new active data file: %w", err)
		}
		db.triggerLogCompaction(nil)
	}

	n, err := db.fwEncoder.Encode(rec)
	if err != nil {
		if n == 0 {
			return err
		}
		return ErrPartialWrite
	}

	db.fwOffset += n

	v := indexedValue{
		FileID: db.fwID,
		Offset: db.fwOffset - int64(len(rec.Value)),
		Size:   len(rec.Value),
	}

	if len(value) == 0 {
		delete(db.kvIndex, key)
	} else {
		db.kvIndex[key] = v
	}

	return nil
}

// Delete deletes the key-value pair associated with key.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if _, ok := db.kvIndex[key]; !ok {
		return nil
	}
	return db.put(key, nil)
}

// Keys iterates over all keys, passing each key to f and terminating when f
// returns false or all keys have been enumerated.
func (db *DB) EachKey(f func(key string) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	for k := range db.kvIndex {
		if !f(k) {
			return nil
		}
	}
	return nil
}

// Sync commits the current contents of the active data file to stable storage.
// Typically, this means flushing the file system's in-memory copy of recently
// written data to disk.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	return db.fw.Sync()
}

// Close closes the database, syncing the active data files, closing all open
// file handles, and releasing it's lock on the database directory. It will not
// return until log compaction has completed if one is running at the time.
//
// Once Close has been called on a [DB], it may not be reused; future calls
// to Close or other methods such as [DB.Get] or [DB.Put] will return
// [ErrDatabaseClosed].
func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}

	// Signal the background goroutine to stop. This must be done *after*
	// releasing the lock and by sending on, rather than closing, the channel in
	// order to wait for any log compaction job waiting to obtain a lock to
	// complete refore we return.
	defer func() { db.closeChan <- struct{}{} }()
	defer db.mu.Unlock()

	db.closed = true

	if err := syncAndClose(db.fw); err != nil {
		return fmt.Errorf("syncing and closing active data file opened for writing: %w", err)
	}

	for _, fr := range db.frIndex {
		_ = fr.Close() // ignore error, read-only file
	}

	err := os.Remove(filepath.Join(db.dir, lockFilename))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing %s file: %v", lockFilename, err)
	}

	return nil
}
