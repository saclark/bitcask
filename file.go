package bitcask

import (
	"fmt"
	"io/fs"
	"os"
)

const (
	lockFilename         = "~.lock"
	dfFileExt            = ".data"
	lockFileFlag         = os.O_CREATE | os.O_EXCL
	dfFileFlag           = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	dbDirMode            = fs.FileMode(0755) // rwxr-xr-x
	lockFileMode         = fs.FileMode(0644) // rw-r--r--
	dfFileMode           = fs.FileMode(0644) // rw-r--r--
	minCompactedFileID   = fileID(0x00)
	minUncompactedFileID = fileID(0x8000000)
)

func syncAndClose(f *os.File) error {
	if err := f.Sync(); err != nil {
		_ = f.Close() // try to close, ignore error
		return fmt.Errorf("syncing %s: %v", f.Name(), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %v", f.Name(), err)
	}
	return nil
}

// fileID is a unique identifier for a data file.
//
// The most significant bit is used to differentiate between compacted and
// uncompacted data files, with 0 denoting a compacted data file and 1 denoting
// an uncompacted data file.
//
// This means values in the ranges [0x00, 0x8000000) and
// [0x8000000, 0xFFFFFFFFFFFFFFFF) represent compacted and uncompacted data
// files, respectively.
//
// Lower fileIDs should be loaded, or "replayed", before higher fileIDs.
type fileID uint64

// Compacted returns whether the fileID represents a compacted data file.
func (id fileID) Compacted() bool {
	return id < minUncompactedFileID
}

// Inc increments the fileID by 1 and panics if doing so would cross over either
// of the compacted/uncompacted fileID boundaries.
func (id fileID) Inc() fileID {
	n := id + 1
	if n == minCompactedFileID || n == minUncompactedFileID {
		panic("fileID overflow")
	}
	return n
}

// Filename returns the filename corresponding to the fileID.
//
// It is formatted such that the lexicographical ordering of data file filenames
// should match the numerical ordering of their corresponding fileIDs.
func (id fileID) Filename() string {
	return fmt.Sprintf("%020d", id) + dfFileExt
}

func (db *DB) nextCompactedFileID() fileID {
	max := minCompactedFileID
	for fid := range db.frIndex {
		if fid.Compacted() && fid > max {
			max = fid
		}
	}
	return max.Inc()
}

func (db *DB) nextUncompactedFileID() fileID {
	max := minUncompactedFileID
	for fid := range db.frIndex {
		if !fid.Compacted() && fid > max {
			max = fid
		}
	}
	return max.Inc()
}
