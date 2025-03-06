package bitcask

import (
	"bytes"
	"strconv"
	"testing"
	"time"
)

func TestWalRecrod_TTL(t *testing.T) {
	tt := []struct {
		expiry        uint64
		wantExpired   bool
		wantHasExpiry bool
	}{
		{0, false, false},
		{uint64(time.Now().Add(1<<63 - 1).Unix()), false, true},
		{uint64(time.Now().Add(5 * time.Minute).Unix()), false, true},
		{uint64(time.Now().Add(-5 * time.Minute).Unix()), true, true},
		{uint64(time.Now().Add(-(1<<63 - 1)).Unix()), true, true},
	}

	for _, tc := range tt {
		t.Run(strconv.FormatUint(tc.expiry, 10), func(t *testing.T) {
			r := walRecord{Expiry: tc.expiry}
			haveTTL, haveHasExpiry := r.TTL()
			if tc.wantExpired && haveTTL != 0 {
				t.Errorf("ttl: want: %d, have: %d", 0, haveTTL)
			}
			if !tc.wantExpired && haveTTL == 0 {
				t.Errorf("ttl: want: %s, have: %d", "ttl > 0", haveTTL)
			}
			if tc.wantHasExpiry != haveHasExpiry {
				t.Errorf("hasExpiry: want: %v, have: %v", tc.wantHasExpiry, haveHasExpiry)
			}
			if !tc.wantHasExpiry && haveTTL != time.Duration(uint64(1<<63-1)) {
				t.Errorf("ttl: want: %s, have: %d", "ttl > 0", haveTTL)
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	var buf bytes.Buffer
	enc := newWALRecordEncoder(&buf)
	rec := newWALRecord([]byte("mykey"), []byte("myvalue"), 0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	record := []byte{0x80, 0x75, 0x01, 0x7b, 0x17, 0xc9, 0xb1, 0xef, 0x01, 0xe3, 0xc5, 0xc8, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x07, 0x6d, 0x79, 0x6b, 0x65, 0x79, 0x6d, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dec := newWALRecordDecoder(bytes.NewBuffer(record))
		b.StartTimer()
		if _, err := dec.Decode(&walRecord{}); err != nil {
			b.Fatal(err)
		}
	}
}
