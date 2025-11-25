package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// RecordDescription specifies the location and size of a record.
type RecordDescription struct {
	SegmentID segmentID // The ID of the segment in which the record is stored.
	Offset    int64     // The byte offset at which the record is stored within the segment.
	Size      int64     // The byte size of the record.
}

// RecordIndexer is an index mapping keys to [RecordDescription]s.
type RecordIndexer interface {
	Get(key string) (RecordDescription, bool)
	Put(key string, value RecordDescription)
	Delete(key string)
}

// TODO: Implement a skip list index.
type mapIndex map[string]RecordDescription

func (m mapIndex) Get(key string) (RecordDescription, bool) {
	v, ok := m[key]
	return v, ok
}

func (m mapIndex) Put(key string, value RecordDescription) {
	m[key] = value
}

func (m mapIndex) Delete(key string) {
	delete(m, key)
}

// Offsets to fields in a binary encoded [indexRecord], for which the format is:
//
//	+-------------+-------------+-----------+---------------+- ... -+
//	| Expiry (8B) | Offset (8B) | Size (8B) | Key Size (4B) |  Key  |
//	+-------------+-------------+-----------+---------------+- ... -+
const (
	irExpOff, irExpEnd = 0, irExpOff + 8
	irOffOff, irOffEnd = irExpEnd, irOffOff + 8
	irSzOff, irSzEnd   = irOffEnd, irSzOff + 8
	irKszOff, irKszEnd = irSzEnd, irKszOff + 4
	irHeaderSize       = irKszEnd
)

// indexRecord specifies the byte offset and size of a [walRecord] within a
// segment file, along with it's expiry.
type indexRecord struct {
	Key    []byte          // The record's key.
	expiry expiryTimestamp // The record's expiry.
	offset uint64          // The byte offset at which the record is stored within its segment.
	size   uint64          // The byte size of the record.
}

func (r *indexRecord) RecordTTL() (ttl time.Duration, hasExpiry bool) {
	return r.expiry.TTL()
}

func (r *indexRecord) RecordOffset() int64 {
	return int64(r.offset) // TODO: Assert cast is safe.
}

func (r *indexRecord) RecordSize() int64 {
	return int64(r.size) // TODO: Assert cast is safe.
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

	header := make([]byte, irHeaderSize)
	binary.BigEndian.PutUint64(header[irExpOff:irExpEnd], rec.expiry.mapToUInt64())
	binary.BigEndian.PutUint64(header[irOffOff:irOffEnd], rec.offset)
	binary.BigEndian.PutUint64(header[irSzOff:irSzEnd], rec.size)
	binary.BigEndian.PutUint32(header[irKszOff:irKszEnd], uint32(len(rec.Key)))

	var nn int
	nn, err = e.bw.Write(header)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing index record header: %w", err)
	}

	nn, err = e.bw.Write(rec.Key)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing index record key: %w", err)
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
	header := make([]byte, irHeaderSize)
	nn, err := io.ReadFull(d.br, header)
	if err != nil {
		return int64(nn), err
	}

	irKsz := binary.BigEndian.Uint32(header[irKszOff:irKszEnd])

	k := make([]byte, irKsz)
	nn, err = io.ReadFull(d.br, k)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	rec.expiry = mapUint64ToExpiry(binary.BigEndian.Uint64(header[irExpOff:irExpEnd]))
	rec.offset = binary.BigEndian.Uint64(header[irOffOff:irOffEnd])
	rec.size = binary.BigEndian.Uint64(header[irSzOff:irSzEnd])
	rec.Key = k

	return n, nil
}
