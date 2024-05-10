package bitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Switchover causes the database to begin writing to a new active data file,
// marking the prior active data file eligible for compaction.
//
// Active data file switchover already happens automatically when the active
// data file reaches the maximum size given by [Config.MaxFileSize]. However,
// this method allows callers to manually trigger a switchover at will.
func (db *DB) Switchover() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	return db.switchover()
}

// switchover switches the database to write to a new active data file. Callers
// must take care to Lock() before calling this method.
func (db *DB) switchover() (err error) {
	fid := db.nextUncompactedFileID()
	fpath := filepath.Join(db.dir, fid.Filename())

	var fw, fr *os.File
	fw, err = os.OpenFile(fpath, dfFileFlag, dfFileMode)
	if err != nil {
		return fmt.Errorf("opening new active data file for writing: %v", err)
	}

	// Attempt to close and remove files if an error occurs.
	defer func() {
		if err != nil {
			_ = fw.Close() // ignore error, nothing was written to it
			if fr != nil {
				_ = fr.Close() // ignore error, read-only file
			}
			_ = os.Remove(fpath) // ignore error
		}
	}()

	fr, err = os.Open(fpath)
	if err != nil {
		return fmt.Errorf("opening new active data file for reading: %v", err)
	}

	if err := syncAndClose(db.fw); err != nil {
		return fmt.Errorf("syncing and closing active data file opened for writing: %v", err)
	}

	db.fw = fw
	db.fwEncoder = newDataRecordEncoder(fw)
	db.fwOffset = 0
	db.fwID = fid
	db.frIndex[fid] = fr

	return nil
}

// Compact runs a log compaction job unless one is already running, waiting to
// run, or the [DB] is closing. If run, it returns true and any error
// encountered. Otherwise, it immediately returns false and the error is nil.
//
// Unless [Config.ManualCompactionOnly] is true, log compaction already runs
// automatically in the background when the active data file reaches the maximum
// size given by [Config.MaxFileSize]. However, this method allows callers to
// manually trigger a log compaction at will.
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

	var firstNewCompactedFileID fileID
	activeFileID := db.fwID

	var fid fileID
	var fw *os.File
	var offset int64
	for k, v := range db.kvIndex {
		// Skip any values persisted to the active data file or later at the
		// time this log compaction was kicked off.
		if v.FileID >= activeFileID {
			continue
		}

		vfr, ok := db.frIndex[v.FileID]
		if !ok {
			continue
		}

		headerAndKeySize := headerSize + int64(len(k))
		recordSize := headerAndKeySize + int64(v.Size)

		// Create a new data file if necessary.
		if fw == nil || offset+recordSize > db.cfg.MaxFileSize {
			fid = db.nextCompactedFileID()
			fn := fid.Filename()

			if fw != nil {
				if err := syncAndClose(fw); err != nil {
					return fmt.Errorf("syncing and closing compacted data file opened for writing: %v", err)
				}
			} else {
				firstNewCompactedFileID = fid
			}

			var err error
			fw, err = os.OpenFile(filepath.Join(db.dir, fn), dfFileFlag, dfFileMode)
			if err != nil {
				return fmt.Errorf("opening new compacted data file for writing: %v", err)
			}

			fr, err := os.Open(filepath.Join(db.dir, fn))
			if err != nil {
				_ = fw.Close() // ignore error, nothing was written to it
				return fmt.Errorf("opening new compacted data file for reading: %v", err)
			}

			db.frIndex[fid] = fr
			offset = 0
		}

		// Copy the record to the new compacted data file.
		r := io.NewSectionReader(vfr, v.Offset-headerAndKeySize, recordSize)
		written, err := io.Copy(fw, r)
		if err != nil {
			_ = syncAndClose(fw) // TODO: Don't ignore this error if it was a Sync error.
			return fmt.Errorf("copying record to new compacted data file: %v", err)
		}

		offset += written

		// Update the index.
		v.FileID = fid
		v.Offset = offset - int64(v.Size)
		db.kvIndex[k] = v
	}

	if fw != nil {
		if err := syncAndClose(fw); err != nil {
			return fmt.Errorf("syncing and closing compacted data file opened for writing: %v", err)
		}
	}

	for fid, fr := range db.frIndex {
		if fid < firstNewCompactedFileID || (!fid.Compacted() && fid < activeFileID) {
			delete(db.frIndex, fid)
			_ = fr.Close()           // ignore error, read-only file
			_ = os.Remove(fr.Name()) // ignore error
		}
	}

	return nil
}
