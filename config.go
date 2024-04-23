package bitcask

import "fmt"

const (
	defaultMaxKeySize   = 1 << 10 // 1 KiB
	defaultMaxValueSize = 1 << 29 // 512 MiB
	defaultMaxFileSize  = 1 << 32 // 4 GiB
)

// Config configures a [DB].
type Config struct {
	// MaxKeySize specifies the maximum key byte size. If <= 0, the max key byte
	// size is 1024 (1 KiB).
	MaxKeySize int

	// MaxValueSize specifies the maximum value byte size. If <= 0, the max
	// value byte size is 536870912 (512 MiB).
	MaxValueSize int

	// MaxFileSize specifies the maximum data file byte size. If <= 0, the max
	// data file byte size is 4294967296 (4 GiB).
	//
	// Writes are routed to a new active data file and a background log
	// compaction is triggered before the active data file reaches this
	// size. Setting this to a value larger than available disk space
	// effectively turns off automatic active data file switchover and log
	// compaction, in which case both can be performed manually via
	// [DB.Switchover] and [DB.Compact].
	//
	// It must be at least 20 bytes larger than the sum of [Config.MaxKeySize]
	// and [Config.MaxValueSize] in order to accomodate the maximum size data
	// record.
	MaxFileSize int64

	// ManualCompactionOnly turns off automatic log compaction when true,
	// requiring any log compaction to be triggered manually via [DB.Compact].
	ManualCompactionOnly bool

	// UseRWLock specifies whether to use a reader-writer lock for
	// synchronization. When false, a standard mutex is used instead.
	UseRWLock bool

	// HandleEvent handles emitted events, such as when a log compaction has
	// begun, succeeded, or failed. When nil, all events are dropped.
	HandleEvent func(event any)
}

func (c Config) validate() error {
	if maxRecordSize := headerSize + int64(c.MaxKeySize) + int64(c.MaxValueSize); maxRecordSize > c.MaxFileSize {
		return fmt.Errorf(
			"config MaxFileSize (%dB) insufficient to accomodate max record size (%d bytes = %d byte header + %d byte MaxKeySize + %d byte MaxValueSize)",
			c.MaxFileSize,
			maxRecordSize,
			headerSize,
			c.MaxKeySize,
			c.MaxValueSize,
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
	if c.MaxFileSize <= 0 {
		c.MaxFileSize = defaultMaxFileSize
	}
	if c.HandleEvent == nil {
		c.HandleEvent = func(event any) {}
	}
	return c
}

// DefaultConfig is a [Config] with default values.
func DefaultConfig() Config {
	return Config{
		MaxKeySize:           defaultMaxKeySize,
		MaxValueSize:         defaultMaxValueSize,
		MaxFileSize:          defaultMaxFileSize,
		ManualCompactionOnly: false,
		UseRWLock:            false,
		HandleEvent:          func(event any) {},
	}
}
