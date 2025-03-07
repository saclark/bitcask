package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func openTmpDB(t *testing.T, pattern string, config Config) (string, *DB, error) {
	t.Helper()
	currentDir, err := os.Getwd()
	if err != nil {
		return "", nil, fmt.Errorf("getting current working directory: %w", err)
	}
	path, err := os.MkdirTemp(currentDir, pattern)
	if err != nil {
		return "", nil, fmt.Errorf("making tmp direcory: %w", err)
	}
	db, err := Open(path, config)
	if err != nil {
		os.RemoveAll(path)
		return "", nil, fmt.Errorf("opening DB directory: %w", err)
	}
	return path, db, nil
}

func assertOp(t *testing.T, db *DB, op string, key string, value []byte, ttl time.Duration, opErr error) {
	t.Helper()
	switch op {
	case "Get":
		v, err := db.Get(key)
		if !errors.Is(err, opErr) {
			t.Fatalf("Get('%s'): got err '%v', want err '%v'", key, err, opErr)
		}
		if !bytes.Equal(v, value) {
			t.Fatalf("Get('%s'): got '%s', want '%s'", key, v, value)
		}
	case "Put":
		if err := db.Put(key, value, ttl); !errors.Is(err, opErr) {
			t.Fatalf("Put('%s', '%s', '%v'): got err '%v', want err '%v'", key, value, ttl, err, opErr)
		}
	case "Del":
		if err := db.Delete(key); !errors.Is(err, opErr) {
			t.Fatalf("Delete('%s'): got err '%v', want err '%v'", key, err, opErr)
		}
	default:
		panic("invalid operation")
	}
}

func TestOpen_LocksDB(t *testing.T) {
	config := DefaultConfig()
	path, db, err := openTmpDB(t, "TestOpen_LocksDB", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory the first time: %v", err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	want := ErrDatabaseLocked
	_, err = Open(path, config)
	if !errors.Is(err, want) {
		t.Fatalf("want '%v', got '%v'", want, err)
	}
}

func TestOpen_FailsAfterLockingDB_UnlocksDB(t *testing.T) {
	// Create a DB.
	config := DefaultConfig()
	path, db, err := openTmpDB(t, "TestOpen_FailsAfterLockingDB_UnlocksDB", config)
	if err != nil {
		t.Fatalf("setting up tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	// Insert an improperly named segment file, causing Open to fail after it's
	// already obtained a lock on the dir.
	invalidSegPath := filepath.Join(path, "invalid.seg")
	if _, err := os.Create(invalidSegPath); err != nil {
		t.Fatalf("creating invalid segment file: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("closing the database: %v", err)
	}

	// Try (and fail) to open the DB.
	db, err = Open(path, config)
	if err == nil {
		db.Close()
		t.Fatal("test setup failed: open succeeded by was expected to fail")
	}
	if errors.Is(err, ErrDatabaseLocked) {
		t.Fatal("test setup failed: database already locked")
	}

	// Remove the invalid segment so the subsequent Open can succeed.
	if err := os.Remove(invalidSegPath); err != nil {
		t.Fatalf("test setup failed: failed to remove invalid segment file: %v", err)
	}

	// Should succeed if the lock file was cleaned up by the prior Open as
	// expected.
	db, err = Open(path, config)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()
}

func TestDB_SingleThreaded(t *testing.T) {
	config := DefaultConfig()
	config.MaxKeySize = 4
	config.MaxValueSize = 8
	config.MaxSegmentSize = 64
	config.HandleEvent = func(event any) {
		switch ev := event.(type) {
		case error:
			t.Logf("event: error: %s\n", ev)
		default:
			t.Logf("event: %v\n", ev)
		}
	}

	path, db, err := openTmpDB(t, "TestDB_SingleThreaded", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	segs, err := filepath.Glob(filepath.Join(path, "*.seg"))
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	if want := 1; len(segs) != want {
		t.Fatalf("want %d file, got %d", want, len(segs))
	}

	ops := []struct {
		Op    string
		Key   string
		Value []byte
		Err   error
	}{
		{"Put", "aaaa", []byte("v1aaaaaa"), nil},               // 32 = 20 + 12
		{"Put", "bbbb", []byte("v1bbbbbb"), nil},               // 32 = 20 + 12
		{"Put", "cccc", []byte("v1cc"), nil},                   // 28 = 20 + 8
		{"Get", "aaaa", []byte("v1aaaaaa"), nil},               // 0
		{"Put", "aaaa", []byte("v2aaaaaa"), nil},               // 32 = 20 + 12
		{"Del", "bbbb", nil, nil},                              // 24 = 20 + 4
		{"Put", "dddd", []byte("v1dd"), nil},                   // 28 = 20 + 8
		{"Get", "aaaa", []byte("v2aaaaaa"), nil},               // 0
		{"Put", "eeee", []byte("v1ee"), nil},                   // 28 = 20 + 8
		{"Put", "ffff", []byte("v1ff"), nil},                   // 28 = 20 + 8
		{"Get", "bbbb", nil, ErrKeyNotFound},                   // 0
		{"Del", "x", nil, nil},                                 // 0
		{"Put", "aaaaa", []byte("v3aaaaaa"), ErrKeyTooLarge},   // 0
		{"Put", "aaaa", []byte("v4aaaaaaa"), ErrValueTooLarge}, // 0
		{"Get", "z", nil, ErrKeyNotFound},                      // 0
		{"Get", "cccc", []byte("v1cc"), nil},                   // 0
		{"Get", "ffff", []byte("v1ff"), nil},                   // 0
		{"Get", "aaaa", []byte("v2aaaaaa"), nil},               // 0
	}

	for i, op := range ops {
		t.Logf("i=%d", i)
		assertOp(t, db, op.Op, op.Key, op.Value, 0, op.Err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("closing DB: %v", err)
	}

	segs, err = filepath.Glob(filepath.Join(path, "*.seg"))
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}

	if want := 3; len(segs) != want {
		t.Fatalf("want %d files, got %d", want, len(segs))
	}

	for _, seg := range segs {
		info, err := os.Stat(seg)
		if err != nil {
			t.Fatalf("statting %s: %v", info.Name(), err)
		}
		if info.Size() > config.MaxSegmentSize {
			t.Fatalf("%s: want size <= %d, got size: %d", info.Name(), config.MaxSegmentSize, info.Size())
		}
	}

	db, err = Open(path, config)
	if err != nil {
		t.Fatalf("re-opening DB: %v", err)
	}
	defer db.Close()

	ops = []struct {
		Op    string
		Key   string
		Value []byte
		Err   error
	}{
		{"Get", "aaaa", []byte("v2aaaaaa"), nil},
		{"Get", "bbbb", nil, ErrKeyNotFound},
		{"Get", "z", nil, ErrKeyNotFound},
		{"Get", "cccc", []byte("v1cc"), nil},
		{"Get", "ffff", []byte("v1ff"), nil},
		{"Get", "aaaa", []byte("v2aaaaaa"), nil},
	}

	for i, op := range ops {
		t.Logf("i=%d", i)
		assertOp(t, db, op.Op, op.Key, op.Value, 0, op.Err)
	}
}

func TestGet_WhileCompactingLog(t *testing.T) {
	const vsize = 4096
	config := DefaultConfig()
	config.MaxKeySize = 1
	config.MaxValueSize = vsize
	config.MaxSegmentSize = 1 + vsize + 20

	var compactErrs []error
	config.HandleEvent = func(event any) {
		switch ev := event.(type) {
		case error:
			if ev != nil && !errors.Is(ev, &LogCompactionError{}) {
				compactErrs = append(compactErrs, ev)
			}
		}
	}

	path, db, err := openTmpDB(t, "TestGet_WhileCompactingLog", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	err = db.Put("a", bytes.Repeat([]byte{'a'}, vsize), 0)
	if err != nil {
		t.Fatalf("Put(\"a\", ...): %v", err)
	}

	want := bytes.Repeat([]byte{'b'}, vsize)
	c := make(chan error)
	go func() {
		c <- nil
		for range 10 {
			if err := db.Put("b", want, 0); err != nil {
				c <- err
				return
			}
		}
		c <- nil
	}()

	<-c
	for range 20 {
		got, err := db.Get("b")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatalf("want %s, got %s", want, got)
		}
	}
	if err := <-c; err != nil {
		t.Fatalf("Put(\"b\", ...): %v", err)
	}
	if len(compactErrs) > 0 {
		for _, err := range compactErrs {
			t.Errorf("compact: %v", err)
		}
	}
}

func TestGet_TruncatedRecord(t *testing.T) {
	path, db, err := openTmpDB(t, "TestGet_TruncatedRecord", DefaultConfig())
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	k1, v1 := "aaaa", []byte("bbbb")
	if err := db.Put(k1, v1, 0); err != nil {
		t.Fatalf("Put('%s', '%s'): %v", k1, v1, err)
	}

	k2, v2 := "cccc", []byte("dddd")
	if err := db.Put(k2, v2, 0); err != nil {
		t.Fatalf("Put('%s', '%s'): %v", k2, v2, err)
	}

	if got, err := db.Get(k1); err != nil {
		t.Fatalf("Get('%s'): %s", k1, err)
	} else if !bytes.Equal(v1, got) {
		t.Fatalf("want %s, got %s", v1, got)
	}

	if got, err := db.Get(k2); err != nil {
		t.Fatalf("Get('%s'): %s", k2, err)
	} else if !bytes.Equal(v2, got) {
		t.Fatalf("want %s, got %s", v2, got)
	}

	fnames, err := filepath.Glob(filepath.Join(path, "*.seg"))
	if err != nil {
		t.Fatalf("globing dir: %v", err)
	}
	if len(fnames) != 1 {
		t.Fatalf("want 1 segment file, got %d", len(fnames))
	}

	fname := fnames[0]
	info, err := os.Stat(fname)
	if err != nil {
		t.Fatalf("statting file: %v", err)
	}

	fsize := 16 + int64(len(k1)) + int64(len(v1)) + 4 + 16 + int64(len(k2)) + int64(len(v2)) + 4
	if info.Size() != fsize {
		t.Fatalf("want file size %d, got %d", fsize, info.Size())
	}

	for i := int64(1); i < fsize; i++ {
		if err := os.Truncate(fname, fsize-i); err != nil {
			t.Fatalf("truncating file: %v", err)
		}
		if _, err := db.Get(k2); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("%d byte(s) truncated: want error: '%v', got: '%v'", i, io.ErrUnexpectedEOF, err)
		}
	}
}

func TestDelete_NonexistentKey_DoesNotWriteTombstone(t *testing.T) {
	path, db, err := openTmpDB(t, "TestDelete_NonexistentKey_DoesNotWriteTombstone", DefaultConfig())
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)
	defer db.Close()

	for i := 0; i < 255; i++ {
		if err := db.Delete(string([]rune{rune(i)})); err != nil {
			t.Fatalf("error deleting non-existent key: %v", err)
		}
	}

	segs, err := filepath.Glob(filepath.Join(path, "*.seg"))
	if err != nil {
		t.Fatalf("reading directory: %v", err)
	}
	for _, seg := range segs {
		info, err := os.Stat(seg)
		if err != nil {
			t.Fatalf("getting file info: %v", err)
		}
		if got := info.Size(); got > 0 {
			t.Fatalf("wanted empty file, got file size: %d", got)
		}
	}
}

func TestClose_WhilePutting(t *testing.T) {
	config := DefaultConfig()
	config.MaxKeySize = 5000
	config.MaxValueSize = 5000

	tt := []struct {
		name string
		kLen int
		vLen int
	}{
		{
			name: "flushing buffered writer",
			kLen: 2,
			vLen: 2,
		},
		{
			name: "writing key",
			kLen: 5000, // longer than bufio.Writer's default 4096 buffer
			vLen: 2,
		},
		{
			name: "writing value",
			kLen: 2,
			vLen: 5000, // longer than bufio.Writer's default 4096 buffer
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			path, db, err := openTmpDB(t, "TestClose_WhilePutting", config)
			if err != nil {
				t.Fatalf("opening tmp DB directory: %v", err)
			}
			defer os.RemoveAll(path)

			k := strings.Repeat("k", tc.kLen)

			c := make(chan error)
			go func() {
				c <- nil
				for i := 0; ; i++ {
					if err := db.Put(k, bytes.Repeat([]byte{byte(i)}, tc.vLen), 0); err != nil {
						c <- err
						return
					}
				}
			}()

			<-c
			if err := db.Close(); err != nil {
				t.Fatalf("Close(): %v", err)
			}
			if err := <-c; !errors.Is(err, ErrDatabaseClosed) {
				t.Fatalf("Put(): %v", err)
			}

			// Ensure all records are valid.
			if _, err := Open(path, config); err != nil {
				t.Fatalf("failed to Open() after Close(): %v", err)
			}
		})
	}
}

func TestClose_WhileGetting(t *testing.T) {
	path, db, err := openTmpDB(t, "TestClose_WhileGetting", DefaultConfig())
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	k, v := "foo", []byte("bar")
	if err := db.Put(k, v, 0); err != nil {
		t.Fatalf("Put(): %v", err)
	}

	c := make(chan error)
	go func() {
		c <- nil
		for i := 0; ; i++ {
			got, err := db.Get(k)
			if err != nil {
				c <- err
				return
			}
			if !bytes.Equal(v, got) {
				c <- fmt.Errorf("want: %s, got: %s", v, got)
				return
			}
		}
	}()

	<-c
	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-c; !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Get(): %v", err)
	}

	// Ensure all records are valid.
	if _, err := Open(path, DefaultConfig()); err != nil {
		t.Fatalf("failed to Open() after Close(): %v", err)
	}
}

func TestClose_WhileDeleting(t *testing.T) {
	path, db, err := openTmpDB(t, "TestClose_WhileDeleting", DefaultConfig())
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	v := []byte("v")
	for i := 0; i < 1000; i++ {
		if err := db.Put(strconv.Itoa(i), v, 0); err != nil {
			t.Fatalf("Put(): %v", err)
		}
	}

	c := make(chan error)
	go func() {
		c <- nil
		for i := 0; i < 1000; i++ {
			if err := db.Delete(strconv.Itoa(i)); err != nil {
				c <- err
				return
			}
		}
		c <- errors.New("delete loop completed before Close() was called")
	}()

	<-c
	if err := db.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := <-c; !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Delete(): %v", err)
	}

	// Ensure all records are valid.
	if _, err := Open(path, DefaultConfig()); err != nil {
		t.Fatalf("failed to Open() after Close(): %v", err)
	}
}

// TODO: Test Close() while compacting.
func TestClose_WhileCompacting(t *testing.T) {
	t.Skip("TODO")
}

func TestClose_UnlocksDB(t *testing.T) {
	config := DefaultConfig()
	path, db, err := openTmpDB(t, "TestClose_UnlocksDB", config)
	if err != nil {
		t.Fatalf("opening tmp DB directory the first time: %v", err)
	}
	defer os.RemoveAll(path)

	if _, err := os.Stat(filepath.Join(path, "~.lock")); err != nil {
		t.Fatalf("verifying ~.lock file exists: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := os.Stat(filepath.Join(path, "~.lock")); !os.IsNotExist(err) {
		t.Fatalf("want: '%v', got: '%v'", fs.ErrNotExist, err)
	}
}

func TestClose_DBMethodsReturnErrDatabaseClosed(t *testing.T) {
	path, db, err := openTmpDB(t, "TestClose_DBMethodsReturnErrDatabaseClosed", DefaultConfig())
	if err != nil {
		t.Fatalf("opening tmp DB directory: %v", err)
	}
	defer os.RemoveAll(path)

	if err := db.Close(); err != nil {
		t.Fatalf("closing DB the first time: %v", err)
	}

	if _, err := db.Get("foo"); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Get: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.Put("foo", []byte("bar"), 0); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Put: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.Delete("foo"); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Delete: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.EachKey(func(key string) bool { return false }); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("EachKey: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.RotateSegment(); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("RotateSegment: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if _, err := db.CompactLog(); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("CompactLog: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.Sync(); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Sync: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
	if err := db.Close(); !errors.Is(err, ErrDatabaseClosed) {
		t.Errorf("Close: want '%v', got '%v'", ErrDatabaseClosed, err)
	}
}

func BenchmarkGet(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}
	testDir, err := os.MkdirTemp(currentDir, "BenchmarkGet")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	tt := []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tc := range tt {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))

			key := "foo"
			value := []byte(strings.Repeat(" ", tc.size))

			config := DefaultConfig()
			config.MaxKeySize = len(key)
			config.MaxValueSize = tc.size

			db, err := Open(testDir, config)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			err = db.Put(key, value, 0)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				if err != nil {
					b.Fatal(err)
				}
				if !bytes.Equal(val, value) {
					b.Errorf("unexpected value")
				}
			}
		})
	}
}

func BenchmarkPut(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	tt := []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	testDir, err := os.MkdirTemp(currentDir, "BenchmarkPut")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	db, err := Open(testDir, DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tt {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))
			key := "foo"
			value := []byte(strings.Repeat(" ", tc.size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := db.Put(key, value, 0); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPutSync(b *testing.B) {
	currentDir, err := os.Getwd()
	if err != nil {
		b.Fatal(err)
	}

	tt := []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	testDir, err := os.MkdirTemp(currentDir, "BenchmarkPutSync")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	db, err := Open(testDir, DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tt {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(tc.size))
			key := "foo"
			value := []byte(strings.Repeat(" ", tc.size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := db.Put(key, value, 0); err != nil {
					b.Fatal(err)
				}
				if err = db.Sync(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
