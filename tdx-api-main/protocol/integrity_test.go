package protocol

import (
	"encoding/binary"
	"math"
	"testing"
	"time"
)

func TestPackedSmallAmounts(t *testing.T) {
	for _, want := range []float32{0, 1, 12.8, 13, 14.53, 15, 43.5, 64, 100, 128, 1000, 11651904} {
		got := getVolume(math.Float32bits(want))
		if got != float64(want) {
			t.Errorf("packed %g: got %g", want, got)
		}
	}
}

func minuteFixture(shares, amount float32) []byte {
	b := []byte{1, 0}
	day := uint16((2026-2004)*2048 + 910)
	stamp := make([]byte, 4)
	binary.LittleEndian.PutUint16(stamp, day)
	binary.LittleEndian.PutUint16(stamp[2:], 11*60+21)
	b = append(b, stamp...)
	// Price=12800 li, followed by three zero deltas.
	b = append(b, 0x80, 0xc8, 0x01, 0, 0, 0)
	f := make([]byte, 8)
	binary.LittleEndian.PutUint32(f, math.Float32bits(shares))
	binary.LittleEndian.PutUint32(f[4:], math.Float32bits(amount))
	return append(b, f...)
}

func TestKlineTruncationNeverReturnsFacts(t *testing.T) {
	b := minuteFixture(1, 13)
	for n := 0; n < len(b); n++ {
		func() {
			defer func() {
				if e := recover(); e != nil {
					t.Errorf("truncated %d panicked: %v", n, e)
				}
			}()
			if r, e := MKline.Decode(b[:n], KlineCache{Type: TypeKlineMinute, Kind: KindStock}); e == nil {
				t.Errorf("truncated %d returned %#v", n, r)
			}
		}()
	}
}

func TestMinuteWirePreservesOddSharesAndAmounts(t *testing.T) {
	for _, shares := range []float32{1, 101, 137, 199} {
		r, err := MKline.Decode(minuteFixture(shares, 13), KlineCache{Type: TypeKlineMinute, Kind: KindStock})
		if err != nil {
			t.Fatal(err)
		}
		k := r.List[0]
		if k.Amount != 13000 || k.VolumeShares == nil || *k.VolumeShares != int64(shares) || k.Volume != int64(shares)/100 || k.Open != 12800 {
			t.Fatalf("units: %#v", k)
		}
	}
}

func TestProviderZeroVolumeWithPositiveAmountIsNotExactZeroShares(t *testing.T) {
	r, err := MKline.Decode(minuteFixture(0, 13), KlineCache{Type: TypeKlineMinute, Kind: KindStock})
	if err != nil {
		t.Fatal(err)
	}
	if r.List[0].VolumeShares != nil {
		t.Fatal("upstream lost odd-lot volume was certified as exact zero shares")
	}
}

func TestInvalidWireDateCannotRollIntoAnotherDay(t *testing.T) {
	for _, minute := range []bool{true, false} {
		b := minuteFixture(1, 13)
		typ := TypeKlineMinute
		if minute {
			binary.LittleEndian.PutUint16(b[2:4], uint16((2026-2004)*2048+931))
		} else {
			typ = TypeKlineDay
			binary.LittleEndian.PutUint32(b[2:6], 20260931)
		}
		if _, err := MKline.Decode(b, KlineCache{Type: typ, Kind: KindStock}); err == nil {
			t.Fatal("invalid September 31 normalized into October")
		}
	}
}

func TestIntradayHeaderAndCumulativePrice(t *testing.T) {
	b := []byte{2, 0, 0, 0, 0xa4, 0x13, 0, 1, 1, 0, 2} // 1252 cents, then +1 cent.
	r, err := MMinute.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if r.List[0].Price != 12520 || r.List[1].Price != 12530 || r.List[0].Time != "09:31" {
		t.Fatalf("intraday units/time: %#v", r)
	}
}

func TestMinuteHeadersCannotManufactureZeroRows(t *testing.T) {
	for _, decode := range []func([]byte) (*MinuteResp, error){MMinute.Decode, MHistoryMinute.Decode} {
		if _, err := decode([]byte{240, 0, 0, 0, 0, 0}); err == nil {
			t.Error("empty payload accepted as 240 facts")
		}
	}
}

func TestShanghaiTimeIsIndependentOfProcessTimezone(t *testing.T) {
	prior := time.Local
	time.Local = time.UTC
	defer func() { time.Local = prior }()
	d := make([]byte, 4)
	binary.LittleEndian.PutUint32(d, 20260910)
	_, offset := GetTime([4]byte(d), TypeKlineDay).Zone()
	if offset != 8*3600 {
		t.Errorf("exchange offset=%d", offset)
	}
}
