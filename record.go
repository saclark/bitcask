package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// [walRecord] header offsets
const (
	crcOff     = 0
	tsOff      = crcOff + 4
	kszOff     = tsOff + 8
	vszOff     = kszOff + 4
	headerSize = vszOff + 4
)

// walRecord represents the data written to a segment file for a single
// key-value pair.
type walRecord struct {
	CRC       uint32
	Timestamp uint64
	Key       []byte
	Value     []byte
}

// newWALRecord constructs a new [walRecord] value.
func newWALRecord(key []byte, value []byte) walRecord {
	return walRecord{
		CRC:       crc32.ChecksumIEEE(value),
		Timestamp: uint64(time.Now().UTC().UnixNano()),
		Key:       key,
		Value:     value,
	}
}

// Size returns the byte size of the full [walRecord] when written to the segment
// file.
func (r *walRecord) Size() int64 {
	return headerSize + int64(len(r.Key)) + int64(len(r.Value))
}

// Valid computes a 32-bit CRC checksum of r.Value and returns whether it
// matches r.CRC.
func (r *walRecord) Valid() bool {
	return crc32.ChecksumIEEE(r.Value) == r.CRC
}

// walRecordEncoder writes [walRecord] values to an output stream.
type walRecordEncoder struct {
	w  io.Writer
	bw *bufio.Writer
}

// newWALRecordEncoder returns a new walRecordEncoder that writes to w.
func newWALRecordEncoder(w io.Writer) *walRecordEncoder {
	return &walRecordEncoder{w: w, bw: bufio.NewWriter(w)}
}

// Encode writes the binary encoding of rec to the stream.
func (e *walRecordEncoder) Encode(rec walRecord) (n int64, err error) {
	// If there is an error, reset the buffered writer and adjust n to reflect
	// the number of bytes actually flushed to the underlying writer.
	defer func() {
		if err != nil {
			n = n - int64(e.bw.Buffered())
			if n < 0 {
				n = 0
			}
			e.bw.Reset(e.w)
		}
	}()

	header := make([]byte, headerSize)
	binary.BigEndian.PutUint32(header[crcOff:tsOff], rec.CRC)
	binary.BigEndian.PutUint64(header[tsOff:kszOff], rec.Timestamp)
	binary.BigEndian.PutUint32(header[kszOff:vszOff], uint32(len(rec.Key)))
	binary.BigEndian.PutUint32(header[vszOff:headerSize], uint32(len(rec.Value)))

	var nn int
	nn, err = e.bw.Write(header)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing record header: %w", err)
	}

	nn, err = e.bw.Write(rec.Key)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing key: %w", err)
	}

	nn, err = e.bw.Write(rec.Value)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing value: %w", err)
	}

	if err := e.bw.Flush(); err != nil {
		return n, fmt.Errorf("flushing buffered writer: %w", err)
	}

	return n, nil
}

// walRecordDecoder reads and decodes [walRecord] values from an input stream.
type walRecordDecoder struct {
	r  io.Reader
	br *bufio.Reader
}

// newWALRecordDecoder returns a new walRecordDecoder that reads from r.
func newWALRecordDecoder(r io.Reader) *walRecordDecoder {
	return &walRecordDecoder{r: r, br: bufio.NewReader(r)}
}

// Decode reads the next encoded [walRecord] value from its input and stores it
// in the value pointed to by rec.
func (d *walRecordDecoder) Decode(rec *walRecord) (n int64, err error) {
	// If there is an error, reset the buffered reader and adjust n to reflect
	// the number of bytes actually read from the underlying reader.
	defer func() {
		if err != nil {
			n = n + int64(d.br.Buffered())
			d.br.Reset(d.r)
		}
	}()

	header := make([]byte, headerSize)
	nn, err := io.ReadFull(d.br, header)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	ksz := binary.BigEndian.Uint32(header[kszOff:vszOff])
	vsz := binary.BigEndian.Uint32(header[vszOff:headerSize])

	data := make([]byte, ksz+vsz)
	nn, err = io.ReadFull(d.br, data)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	rec.CRC = binary.BigEndian.Uint32(header[crcOff:tsOff])
	rec.Timestamp = binary.BigEndian.Uint64(header[tsOff:kszOff])
	rec.Key = data[:ksz]
	rec.Value = data[ksz : ksz+vsz]

	return n, nil
}
