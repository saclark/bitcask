package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// dataRecord header offsets
const (
	crcOff     = 0
	tsOff      = crcOff + 4
	kszOff     = tsOff + 8
	vszOff     = kszOff + 4
	headerSize = vszOff + 4
)

// dataRecord represents the data written to a data file for a single key-value
// pair.
type dataRecord struct {
	CRC       uint32
	Timestamp uint64
	Key       []byte
	Value     []byte
}

// newDataRecord constructs a new dataRecord value.
func newDataRecord(key []byte, value []byte) dataRecord {
	return dataRecord{
		CRC:       crc32.ChecksumIEEE(value),
		Timestamp: uint64(time.Now().UTC().UnixNano()),
		Key:       key,
		Value:     value,
	}
}

// Size returns the byte size of the full dataRecord when written to the data
// file.
func (r *dataRecord) Size() int64 {
	return headerSize + int64(len(r.Key)) + int64(len(r.Value))
}

// Valid computes a 32-bit CRC checksum of r.Value and returns whether it
// matches r.CRC.
func (r *dataRecord) Valid() bool {
	return crc32.ChecksumIEEE(r.Value) == r.CRC
}

// dataRecordEncoder writes dataRecord values to an output stream.
type dataRecordEncoder struct {
	w  io.Writer
	bw *bufio.Writer
}

// newDataRecordEncoder returns a new dataRecordEncoder that writes to w.
func newDataRecordEncoder(w io.Writer) *dataRecordEncoder {
	return &dataRecordEncoder{w: w, bw: bufio.NewWriter(w)}
}

// Encode writes the binary encoding of rec to the stream.
func (e *dataRecordEncoder) Encode(rec dataRecord) (n int64, err error) {
	// If there is an error, reset the buffered writer and adjust n to reflect
	// the number of bytes actually flushed to the underlying writer.
	defer func() {
		if err != nil {
			flushed := n - int64(e.bw.Buffered())
			e.bw.Reset(e.w)
			if flushed < 0 {
				n = 0
			}
			n = flushed
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

// dataRecordDecoder reads and decodes dataRecord values from an input stream.
type dataRecordDecoder struct {
	r  io.Reader
	br *bufio.Reader
}

// newDataRecordDecoder returns a new dataRecordDecoder that reads from r.
func newDataRecordDecoder(r io.Reader) *dataRecordDecoder {
	return &dataRecordDecoder{r: r, br: bufio.NewReader(r)}
}

// Decode reads the next encoded dataRecord value from its input and stores it
// in the value pointed to by rec.
func (d *dataRecordDecoder) Decode(rec *dataRecord) (err error) {
	defer func() {
		if err != nil {
			d.br.Reset(d.r)
		}
	}()

	header := make([]byte, headerSize)
	if _, err = io.ReadFull(d.br, header); err != nil {
		return err
	}

	ksz := binary.BigEndian.Uint32(header[kszOff:vszOff])
	vsz := binary.BigEndian.Uint32(header[vszOff:headerSize])

	data := make([]byte, ksz+vsz)
	if _, err = io.ReadFull(d.br, data); err != nil {
		return err
	}

	rec.CRC = binary.BigEndian.Uint32(header[crcOff:tsOff])
	rec.Timestamp = binary.BigEndian.Uint64(header[tsOff:kszOff])
	rec.Key = data[:ksz]
	rec.Value = data[ksz : ksz+vsz]

	return nil
}
