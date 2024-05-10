package bitcask

import (
	"errors"
	"fmt"
)

var (
	ErrKeyTooLarge     = errors.New("key exceeds configured maximum size")
	ErrValueTooLarge   = errors.New("value exceeds configured maximum size")
	ErrKeyNotFound     = errors.New("key not found")
	ErrInvalidRecord   = errors.New("invalid record")
	ErrTruncatedRecord = errors.New("truncated record")
	ErrPartialWrite    = errors.New("wrote partial record")
	ErrDatabaseLocked  = errors.New("database locked")
	ErrDatabaseClosed  = errors.New("database closed")
)

// LogCompactionError signifies that log compaction encountered an error. While
// this means that a full compaction of the segments may not have completed, the
// DB should still be in a consistent state.
type LogCompactionError struct {
	err error
}

func (e *LogCompactionError) Error() string {
	return fmt.Sprintf("log compaction: %v", e.err)
}

func (e *LogCompactionError) Unwrap() error {
	return e.err
}
