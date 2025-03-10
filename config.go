package bitcask

import "fmt"

const (
	defaultMaxKeySize     = 1 << 10 // 1 KiB
	defaultMaxValueSize   = 1 << 29 // 512 MiB
	defaultMaxSegmentSize = 1 << 32 // 4 GiB
)

// Config configures a [DB].
type Config struct {
	// MaxKeySize specifies the maximum key size, in bytes, that may be written
	// to the database. Keys of larger size that exist in the database before
	// opening may still be read. If <= 0, the max key size is 1024 (1 KiB).
	MaxKeySize int

	// MaxValueSize specifies the maximum value size, in bytes, that may be
	// written to the database. Values of larger size that exist in the database
	// before opening may still be read. If <= 0, the max value size is
	// 536870912 (512 MiB).
	MaxValueSize int

	// MaxSegmentSize specifies the maximum segment size, in bytes. If <= 0, the
	// max segment size is 4294967296 (4 GiB).
	//
	// Writes are routed to a new active segment and log compaction is triggered
	// (unless [Config.DisableAutomaticLogCompaction] is true) before the active
	// segment exceeds this size. Setting this to a value larger than available
	// disk space effectively turns off automatic active segment rotation and
	// log compaction, in which case both can be performed manually via
	// [DB.RotateSegment] and [DB.CompactLog].
	//
	// It must be at least 20 bytes larger than the sum of [Config.MaxKeySize]
	// and [Config.MaxValueSize] in order to accomodate the maximum size WAL
	// record.
	MaxSegmentSize int64

	// DisableAutomaticLogCompaction turns off automatic log compaction when
	// true, requiring any log compaction to be triggered manually via
	// [DB.CompactLog]. This allows applications to schedule log compaction for
	// times when the potential memory and/or CPU usage increase is acceptable,
	// such as during off-peak hours.
	//
	// When false, log compaction runs automatically before the active segment
	// exceeds [Config.MaxSegmentSize].
	DisableAutomaticLogCompaction bool

	// UseStandardMutex mandates the usage of a standard mutex for
	// synchronization when true. A reader-writer lock is used when false.
	//
	// A standard mutex may outperform a reader-writer lock in a surprising
	// number of scenarios due to the additional overhead associated with
	// reader-writer locks. Users are encouraged to perform their own benchmarks
	// to determine which is most appropriate for their use case.
	UseStandardMutex bool

	// HandleEvent handles emitted events, such as when a log compaction has
	// begun, succeeded, or failed. When nil, all events are dropped.
	HandleEvent func(event any)
}

func (c Config) validate() error {
	maxRecSize := recordSize(c.MaxKeySize, c.MaxValueSize)
	if maxRecSize > c.MaxSegmentSize {
		return fmt.Errorf(
			"config MaxSegmentSize (%dB) insufficient to accomodate computed max record size (%dB)",
			c.MaxSegmentSize,
			maxRecSize,
		)
	}
	return nil
}

func (c Config) hydrated() Config {
	if c.MaxKeySize <= 0 {
		c.MaxKeySize = defaultMaxKeySize
	}
	if c.MaxValueSize <= 0 {
		c.MaxValueSize = defaultMaxValueSize
	}
	if c.MaxSegmentSize <= 0 {
		c.MaxSegmentSize = defaultMaxSegmentSize
	}
	if c.HandleEvent == nil {
		c.HandleEvent = func(event any) {}
	}
	return c
}

// DefaultConfig is a [Config] with default values.
func DefaultConfig() Config {
	return Config{
		MaxKeySize:                    defaultMaxKeySize,
		MaxValueSize:                  defaultMaxValueSize,
		MaxSegmentSize:                defaultMaxSegmentSize,
		DisableAutomaticLogCompaction: false,
		UseStandardMutex:              false,
		HandleEvent:                   func(event any) {},
	}
}
