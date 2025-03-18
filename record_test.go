package bitcask

import (
	"bytes"
	"math"
	"strconv"
	"testing"
	"time"
)

func TestWalRecrod_TTL(t *testing.T) {
	tt := []struct {
		expiry                 expiryTimestamp
		wantTTLComparisonType  int           // (-1, 0, 1) => (<, ==, >)
		wantTTLComparisonValue time.Duration // value to compare haveTTL to
		wantHasExpiry          bool
	}{
		{
			math.MinInt64,
			0,
			time.Duration(math.MaxInt64),
			false,
		},
		{
			0,
			-1,
			time.Duration(0),
			true,
		},
		{
			math.MaxInt64,
			1,
			time.Duration(0),
			true,
		},
	}

	for _, tc := range tt {
		t.Run(strconv.FormatInt(int64(tc.expiry), 10), func(t *testing.T) {
			r := walRecord{Expiry: tc.expiry}
			haveTTL, haveHasExpiry := r.TTL()

			switch tc.wantTTLComparisonType {
			case -1:
				if haveTTL >= tc.wantTTLComparisonValue {
					t.Errorf("ttl: want: < %d, have: %d", tc.wantTTLComparisonValue, haveTTL)
				}
			case 0:
				if haveTTL != tc.wantTTLComparisonValue {
					t.Errorf("ttl: want: %d, have: %d", tc.wantTTLComparisonValue, haveTTL)
				}
			case 1:
				if haveTTL <= tc.wantTTLComparisonValue {
					t.Errorf("ttl: want: > %d, have: %d", tc.wantTTLComparisonValue, haveTTL)
				}
			}

			if tc.wantHasExpiry != haveHasExpiry {
				t.Errorf("hasExpiry: want: %v, have: %v", tc.wantHasExpiry, haveHasExpiry)
			}
			if !tc.wantHasExpiry && haveTTL != time.Duration(math.MaxInt64) {
				t.Errorf("ttl: want: %s, have: %d", "ttl > 0", haveTTL)
			}
		})
	}
}

func BenchmarkEncode(b *testing.B) {
	var buf bytes.Buffer
	enc := newWALRecordEncoder(&buf)
	rec := newWALRecord([]byte("mykey"), []byte("myvalue"), noExpiry)
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
