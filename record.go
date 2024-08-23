package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

const bufSize = 4096

// Offsets to fields in a binary encoded [walRecord], for which the format is:
//
//	+----------------+---------------+-----------------+- ... -+- ... -+----------+
//	| Timestamp (8B) | Key Size (4B) | Value Size (4B) |  Key  | Value | CRC (4B) |
//	+----------------+---------------+-----------------+- ... -+- ... -+----------+
const (
	tsOff, tsEnd   = 0, kszOff
	kszOff, kszEnd = tsOff + 8, vszOff
	vszOff, vszEnd = kszOff + 4, headerSize
	headerSize     = vszOff + 4
	crcSize        = 4
	metaSize       = headerSize + crcSize
)

var ErrCorruptRecord = errors.New("corrupt record")

func recordSize(keySize, valueSize int) int64 {
	return headerSize + int64(keySize) + int64(valueSize) + crcSize
}

// walRecord represents the data written to a segment for a single key-value
// pair.
type walRecord struct {
	Timestamp uint64
	Key       []byte
	Value     []byte
}

// newWALRecord constructs a new [walRecord] value.
func newWALRecord(key []byte, value []byte) walRecord {
	return walRecord{
		Timestamp: uint64(time.Now().UTC().UnixNano()),
		Key:       key,
		Value:     value,
	}
}

// Size returns the byte size of the full [walRecord] when written to the
// segment.
func (r *walRecord) Size() int64 {
	return headerSize + int64(len(r.Key)) + int64(len(r.Value)) + crcSize
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
	defer func() {
		if err != nil {
			if unflushed := e.bw.Buffered(); unflushed > 0 {
				n -= int64(unflushed)
				e.bw.Reset(e.w)
			}
		}
	}()

	header := make([]byte, headerSize)
	binary.BigEndian.PutUint64(header[tsOff:tsEnd], rec.Timestamp)
	binary.BigEndian.PutUint32(header[kszOff:kszEnd], uint32(len(rec.Key)))
	binary.BigEndian.PutUint32(header[vszOff:vszEnd], uint32(len(rec.Value)))

	h := crc32.NewIEEE()
	mw := io.MultiWriter(e.bw, h)

	var nn int
	nn, err = mw.Write(header)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing record header: %w", err)
	}

	nn, err = mw.Write(rec.Key)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing key: %w", err)
	}

	nn, err = mw.Write(rec.Value)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("writing value: %w", err)
	}

	nn, err = e.bw.Write(h.Sum(nil)) // h.Sum is big-endian
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
	h := crc32.NewIEEE()
	tr := io.TeeReader(d.br, h)

	header := make([]byte, headerSize)
	nn, err := io.ReadFull(tr, header)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	ksz := binary.BigEndian.Uint32(header[kszOff:kszEnd])

	k := make([]byte, ksz)
	nn, err = io.ReadFull(tr, k)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	vsz := binary.BigEndian.Uint32(header[vszOff:vszEnd])

	v := make([]byte, vsz)
	nn, err = io.ReadFull(tr, v)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	crc := make([]byte, crcSize)
	nn, err = io.ReadFull(d.br, crc)
	n += int64(nn)
	if err != nil {
		return n, err
	}

	if h.Sum32() != binary.BigEndian.Uint32(crc) {
		return n, ErrCorruptRecord
	}

	rec.Timestamp = binary.BigEndian.Uint64(header[tsOff:tsEnd])
	rec.Key = k
	rec.Value = v

	return n, nil
}

func readRecordValue(ra io.ReaderAt, recordOff, recordSize int64) ([]byte, error) {
	if recordSize > bufSize {
		return readRecordValueBuffered(ra, recordOff, recordSize)
	}
	return readRecordValueUnbuffered(ra, recordOff, recordSize)
}

// Shouldn't happen, but better than panicing in case it did.
var errInvalidRecordSize = errors.New("bitcask: invalid record size")

func readRecordValueUnbuffered(ra io.ReaderAt, recordOff, recordSize int64) ([]byte, error) {
	if recordSize < metaSize {
		return nil, errInvalidRecordSize
	}

	b := make([]byte, recordSize)
	if _, err := ra.ReadAt(b, recordOff); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	h := crc32.NewIEEE()
	if _, err := h.Write(b[:len(b)-crcSize]); err != nil {
		return nil, err
	}
	if h.Sum32() != binary.BigEndian.Uint32(b[len(b)-crcSize:]) {
		return nil, ErrCorruptRecord
	}

	ksz := binary.BigEndian.Uint32(b[kszOff:kszEnd])
	v := b[headerSize+ksz : len(b)-crcSize]

	return v, nil
}

func readRecordValueBuffered(ra io.ReaderAt, recordOff, recordSize int64) (v []byte, err error) {
	if recordSize < metaSize {
		return nil, errInvalidRecordSize
	}

	sr := io.NewSectionReader(ra, recordOff, recordSize)
	br := bufio.NewReaderSize(sr, bufSize)

	h := crc32.NewIEEE()

	// Read the header and write it to the CRC hash.
	header := make([]byte, headerSize)
	if _, err = io.ReadFull(br, header); err != nil {
		return nil, err
	}
	if _, err = h.Write(header); err != nil {
		return nil, err
	}

	// Copy the key directly to the CRC hash.
	ksz := binary.BigEndian.Uint32(header[kszOff:kszEnd])
	if _, err = io.CopyN(h, br, int64(ksz)); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}

	// Read the value and write it to the CRC hash.
	vsz := binary.BigEndian.Uint32(header[vszOff:vszEnd])
	v = make([]byte, vsz)
	if _, err = io.ReadFull(br, v); err != nil {
		return nil, err
	}
	if _, err = h.Write(v); err != nil {
		return nil, err
	}

	// Read the CRC and compare it to the computed CRC.
	crc := make([]byte, crcSize)
	if _, err = io.ReadFull(br, crc); err != nil {
		return nil, err
	}
	if h.Sum32() != binary.BigEndian.Uint32(crc) {
		return nil, ErrCorruptRecord
	}

	return v, nil
}
