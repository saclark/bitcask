package bitcask

import (
	"bytes"
	"testing"
)

func BenchmarkEncoder(b *testing.B) {
	var buf bytes.Buffer
	enc := newWALRecordEncoder(&buf)
	rec := newWALRecord([]byte("mykey"), []byte("myvalue"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecoder(b *testing.B) {
	data := []byte{0x80, 0x75, 0x01, 0x7b, 0x17, 0xc9, 0xb1, 0xef, 0x01, 0xe3, 0xc5, 0xc8, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x07, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dec := newWALRecordDecoder(bytes.NewBuffer(data))
		b.StartTimer()
		if _, err := dec.Decode(&walRecord{}); err != nil {
			b.Fatal(err)
		}
	}
}
