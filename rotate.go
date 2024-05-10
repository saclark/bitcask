package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
)

// RotateSegment causes the database to begin writing to a new active segment,
// making the old active segment eligible for compaction.
//
// Active segment rotation already happens automatically when the active
// segment reaches the maximum size given by [Config.MaxSegmentSize]. However,
// this method allows callers to manually trigger a rotation at will.
func (db *DB) RotateSegment() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	return db.rotateSegment()
}

// rotateSegment causes the database to begin writing to a new active segment.
// Callers must take care to Lock() before calling this method.
func (db *DB) rotateSegment() (err error) {
	fid := db.nextUncompactedSegmentID()
	fpath := filepath.Join(db.dir, fid.Filename())

	var fw, fr *os.File
	fw, err = os.OpenFile(fpath, segFileFlag, segFileMode)
	if err != nil {
		return fmt.Errorf("opening new active segment file for writing: %v", err)
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
		return fmt.Errorf("opening new active segment file for reading: %v", err)
	}

	if err := syncAndClose(db.fw); err != nil {
		return fmt.Errorf("syncing and closing active segment file opened for writing: %v", err)
	}

	db.fw = fw
	db.fwEncoder = newWALRecordEncoder(fw)
	db.fwOffset = 0
	db.fwID = fid
	db.frIndex[fid] = fr

	return nil
}
