package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"testing"
)

func TestRawSharePrecisionColumns(t *testing.T) {
	v := 925050.0
	k := &Kline{Volume: 9250, VolumeShares: &v, VolumeSharesSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	cols, err := k.RawSharePrecisionColumns()
	if err != nil || cols[0] != v || cols[1] != "tdx_decoded" || cols[2] != k.VolumeSharesSHA256 || k.Volume != 9250 {
		t.Fatalf("precision columns: %v %v", cols, err)
	}
	cols, err = (&Kline{Volume: 9250}).RawSharePrecisionColumns()
	if err != nil || cols[0] != nil || cols[1] != nil || cols[2] != nil {
		t.Fatal("legacy must not synthesize precision")
	}
	for _, invalid := range []float64{math.NaN(), math.Inf(1), -1} {
		k.VolumeShares = &invalid
		if _, err = k.RawSharePrecisionColumns(); err == nil {
			t.Fatal("invalid precision accepted")
		}
	}
	k.VolumeShares = &v
	k.VolumeSharesSHA256 = "missing"
	if _, err = k.RawSharePrecisionColumns(); err == nil {
		t.Fatal("unpinned precision accepted")
	}
	k.VolumeShares = nil
	if _, err = k.RawSharePrecisionColumns(); err == nil {
		t.Fatal("orphan provenance accepted")
	}
}

func TestRawKlineSharePrecisionPreservesLegacyVolumeAndSourceBytes(t *testing.T) {
	// Existing captured protocol fixture, first encoded bar only.
	frame, _ := hex.DecodeString("010078da340198b8018404bc055ee8b3e949ad2b094f")
	for _, typ := range []uint8{TypeKlineDay, TypeKlineDay2, TypeKlineMinute} {
		resp, err := MKline.Decode(frame, KlineCache{Type: typ, Kind: KindStock})
		if err != nil || len(resp.List) != 1 {
			t.Fatalf("decode: %v", err)
		}
		k := resp.List[0]
		decoded := getVolume(Uint32(frame[14:18]))
		wantShares, wantLegacy := decoded*100, int64(decoded)
		if typ != TypeKlineDay {
			wantShares, wantLegacy = decoded, int64(decoded)/100
		}
		if k.Volume != wantLegacy || k.VolumeShares == nil || *k.VolumeShares != wantShares {
			t.Fatalf("lost precision or changed legacy volume: %+v", k)
		}
		pin := sha256.Sum256(append([]byte{typ}, frame[2:]...))
		if k.VolumeSharesSHA256 != fmt.Sprintf("%x", pin) {
			t.Fatal("not pinned to original encoded bar/type")
		}
		if math.IsNaN(*k.VolumeShares) || math.IsInf(*k.VolumeShares, 0) {
			t.Fatal("invalid decoded volume")
		}
	}
}

func TestRawKlineEncodedZeroVolumeIsExactlyZero(t *testing.T) {
	frame, _ := hex.DecodeString("010078da340198b8018404bc055e00000000ad2b094f")
	resp, err := MKline.Decode(frame, KlineCache{Type: TypeKlineDay, Kind: KindStock})
	if err != nil || resp.List[0].Volume != 0 || resp.List[0].VolumeShares == nil || *resp.List[0].VolumeShares != 0 {
		t.Fatalf("encoded zero must stay zero, not decoder subnormal: %v %+v", err, resp)
	}
}

func TestRawKlineIndexDoesNotGetEquitySharePrecision(t *testing.T) {
	frame, _ := hex.DecodeString("010078da340198b8018404bc055ee8b3e949ad2b094f01000200")
	resp, err := MKline.Decode(frame, KlineCache{Type: TypeKlineDay, Kind: KindIndex})
	if err != nil || resp.List[0].VolumeShares != nil || resp.List[0].VolumeSharesSHA256 != "" {
		t.Fatal("index units must not be recast as equity shares")
	}
}

func Test_stockKline_Frame(t *testing.T) {
	//预期0c02000000001c001c002d050000303030303031 0900 0100 0000 0a00 00000000000000000000
	//   0c00000000011c001c002d050000313030303030 0900 0000 0000 0a00 00000000000000000000
	f, _ := MKline.Frame(TypeKlineDay, "sz000001", 0, 10)
	t.Log(f.Bytes().HEX())
}

func Test_stockKline_Decode(t *testing.T) {
	s := "0a0078da340198b8018404bc055ee8b3e949ad2b094f79da34010af801a002cc0260dec949859ded4e7ada34016882028e04e603b8f91e4a111f394f7dda3401e401c20200f604f84d2b4ad4d0444f7eda3401721eaa0268d87bc549ee80e34e7fda34011e288601c601d08db849230ed54e80da3401727c32da013023584999a0784e81da3401147c0ad001d0fa86498d989a4e84da34015e6800d60278c28e491ca6a14e85da340154d001b801da01403e924989d6a54e"
	bs, err := hex.DecodeString(s)
	if err != nil {
		t.Error(err)
		return
	}
	resp, err := MKline.Decode(bs, KlineCache{
		Type: 9,
		Kind: "",
	})
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(len(resp.List))
	for _, v := range resp.List {
		t.Log(v)
	}
}
