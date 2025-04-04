package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Offsets to fields in a binary encoded [indexRecord], for which the format is:
//
//	+-------------+-------------+-----------+
//	| Expiry (8B) | Offset (8B) | Size (8B) |
//	+-------------+-------------+-----------+
const (
	irExpOff, irExpEnd = 0, irOffOff
	irOffOff, irOffEnd = irExpOff + 8, irSzOff
	irSzOff, irSzEnd   = irOffOff + 8, irSz
	irSz               = irSzOff + 8
)

// indexRecord specifies the byte offset and size of a [walRecord] within a
// segment file, along with it's expiry.
type indexRecord struct {
	Expiry expiryTimestamp // The record's expiry.
	Offset uint64          // The byte offset at which the record is stored within its segment.
	Size   uint64          // The byte size of the record.
}

// indexRecordEncoder writes [indexRecord] values to an output stream.
type indexRecordEncoder struct {
	w  io.Writer
	bw *bufio.Writer
}

// newIndexRecordEncoder returns a new indexRecordEncoder that writes to w.
func newIndexRecordEncoder(w io.Writer) *indexRecordEncoder {
	return &indexRecordEncoder{w: w, bw: bufio.NewWriter(w)}
}

// Encode writes the binary encoding of rec to the stream.
func (e *indexRecordEncoder) Encode(rec indexRecord) (n int64, err error) {
	defer func() {
		if err != nil {
			if unflushed := e.bw.Buffered(); unflushed > 0 {
				n -= int64(unflushed)
				e.bw.Reset(e.w)
			}
		}
	}()

	b := make([]byte, irSz)
	binary.BigEndian.PutUint64(b[irExpOff:irExpEnd], rec.Expiry.mapToUInt64())
	binary.BigEndian.PutUint64(b[irOffOff:irOffEnd], rec.Offset)
	binary.BigEndian.PutUint64(b[irSzOff:irSzEnd], rec.Size)

	var nn int
	nn, err = e.bw.Write(b)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing index record: %w", err)
	}

	if err = e.bw.Flush(); err != nil {
		return n, fmt.Errorf("flushing buffered writer: %w", err)
	}

	return n, nil
}

// indexRecordDecoder reads and decodes [indexRecord] values from an input
// stream.
type indexRecordDecoder struct {
	r  io.Reader
	br *bufio.Reader
}

// newIndexRecordDecoder returns a new indexRecordDecoder that reads from r.
func newIndexRecordDecoder(r io.Reader) *indexRecordDecoder {
	return &indexRecordDecoder{r: r, br: bufio.NewReader(r)}
}

// Decode reads the next encoded [indexRecord] value from its input and stores
// it in the value pointed to by rec.
func (d *indexRecordDecoder) Decode(rec *indexRecord) (n int64, err error) {
	b := make([]byte, irSz)
	nn, err := io.ReadFull(d.br, b)
	if err != nil {
		return int64(nn), err
	}

	rec.Expiry = mapUint64ToExpiry(binary.BigEndian.Uint64(b[irExpOff:irExpEnd]))
	rec.Offset = binary.BigEndian.Uint64(b[irOffOff:irOffEnd])
	rec.Size = binary.BigEndian.Uint64(b[irSzOff:irSzEnd])

	return n, nil
}
