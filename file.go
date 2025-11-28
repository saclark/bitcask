package bitcask

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	lockFilename            = "~.lock"
	segFileExt              = ".seg"
	idxFileExt              = ".idx"
	lockFileFlag            = os.O_CREATE | os.O_EXCL
	segFileFlag             = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	idxFileFlag             = os.O_CREATE | os.O_APPEND | os.O_WRONLY
	dbDirMode               = fs.FileMode(0o755) // rwxr-xr-x
	lockFileMode            = fs.FileMode(0o644) // rw-r--r--
	segFileMode             = fs.FileMode(0o644) // rw-r--r--
	idxFileMode             = fs.FileMode(0o644) // rw-r--r--
	minCompactedSegmentID   = segmentID(0x00)
	minUncompactedSegmentID = segmentID(0x8000000)
)

func acquireDirLock(dir *os.File) error {
	f, err := createFile(dir, lockFilename, lockFileFlag, lockFileMode)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrDatabaseLocked
		}
		return err
	}
	defer f.Close()
	return nil
}

func releaseDirLock(dir *os.File) error {
	err := removeFile(dir, filepath.Join(dir.Name(), lockFilename))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type syncError struct {
	err error
}

func (e *syncError) Error() string {
	return fmt.Sprintf("bitcask: file sync failed: %s", e.err)
}

func (e *syncError) Unwrap() error {
	return e.err
}

// createFile creates the named file directly under dir and then calls [os.Sync]
// on dir to ensure durability.
//
// TODO: Consider pre-allocating file space with fallocate() for perf
// optimization?
func createFile(dir *os.File, name string, flag int, perm fs.FileMode) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir.Name(), name), flag, perm)
	if err != nil {
		return nil, fmt.Errorf("opening new file: %w", err)
	}
	if err := dir.Sync(); err != nil {
		return nil, &syncError{
			err: fmt.Errorf("syncing parent directory %s: %w", dir.Name(), err),
		}
	}
	return f, nil
}

// removeFile removes the named file and then calls [os.Sync] on parentDir.
func removeFile(parentDir *os.File, name string) error {
	if err := os.Remove(name); err != nil {
		return fmt.Errorf("removing %s: %w", name, err)
	}
	if err := parentDir.Sync(); err != nil {
		return &syncError{
			err: fmt.Errorf(
				"syncing parent directory %s: %w",
				parentDir.Name(),
				err,
			),
		}
	}
	return nil
}

// syncAndClose calls [os.Sync] and [os.Close] on f if not nil.
func syncAndClose(f *os.File) error {
	if f == nil {
		return nil
	}
	if err := f.Sync(); err != nil {
		_ = f.Close() // try to close, ignore error
		return &syncError{err: fmt.Errorf("syncing %s: %v", f.Name(), err)}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %v", f.Name(), err)
	}
	return nil
}

// segmentID is a unique identifier for a segment.
//
// The most significant bit is used to differentiate between compacted and
// uncompacted segments, with 0 denoting a compacted segment and 1 denoting an
// uncompacted segment.
//
// This means values in the ranges [0x00, 0x8000000) and
// [0x8000000, 0xFFFFFFFFFFFFFFFF) represent compacted and uncompacted segments,
// respectively.
//
// Segments with lower segmentIDs should be loaded, or "replayed", before
// segments with higher segmentIDs.
type segmentID uint64

// Compacted returns whether the segmentID represents a compacted segment.
func (id segmentID) Compacted() bool {
	return id < minUncompactedSegmentID
}

// Inc increments the segmentID by 1 and panics if doing so would cross over
// either of the compacted/uncompacted segmentID boundaries.
func (id segmentID) Inc() segmentID {
	n := id + 1
	if n == minCompactedSegmentID || n == minUncompactedSegmentID {
		panic("segmentID overflow")
	}
	return n
}

// Filename returns the filename corresponding to the segmentID.
//
// It is formatted such that the lexicographical ordering of segment filenames
// should match the numerical ordering of their corresponding segmentIDs.
func (id segmentID) Filename() string {
	return fmt.Sprintf("%020d", id) + segFileExt
}

// IndexFilename returns the filename of the segement index file corresponding
// to the segmentID.
//
// It is formatted such that the lexicographical ordering of segment index
// filenames should match the numerical ordering of their corresponding
// segmentIDs.
func (id segmentID) IndexFilename() string {
	return fmt.Sprintf("%020d", id) + idxFileExt
}

// nextCompactedSegmentID returns the next ID to use in the series of
// monotonically increasing segment IDs representing compacted segments.
// Callers must take care to Lock() before calling this method.
func (db *DB) nextCompactedSegmentID() segmentID {
	max := minCompactedSegmentID
	for sid := range db.frs {
		if sid.Compacted() && sid > max {
			max = sid
		}
	}
	return max.Inc()
}

// nextUncompactedSegmentID returns the next ID to use in the series of
// monotonically increasing segment IDs representing uncompacted segments.
// Callers must take care to Lock() before calling this method.
func (db *DB) nextUncompactedSegmentID() segmentID {
	max := minUncompactedSegmentID
	for sid := range db.frs {
		if !sid.Compacted() && sid > max {
			max = sid
		}
	}
	return max.Inc()
}
