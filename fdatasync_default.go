//go:build !linux && !freebsd && !netbsd && !openbsd && !darwin

package bitcask

import (
	"os"
)

// Fdatasync calls [os.Sync] for this platform.
func Fdatasync(f *os.File) error {
	return f.Sync()
}
