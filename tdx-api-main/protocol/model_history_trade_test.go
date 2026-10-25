package protocol

import (
	"testing"
	"time"
)

func Test_stockHistoryMinuteTrade_Frame(t *testing.T) {
	// 预期 0c 02000000 00 1200 1200 b50f 84da3401 0000 30303030303100006400
	//     0c000000000112001200b50f84da3401000030303030303100006400
	f, err := MHistoryTrade.Frame("20241028", "sz000001", 0, 100)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log(f.Bytes().HEX())
}

func TestHistoryTradeDecodeEmpty(t *testing.T) {
	for _, payload := range [][]byte{{0, 0}, {0, 0, 0, 0, 0, 0}} {
		resp, err := MHistoryTrade.Decode(payload, TradeCache{Date: "20240122", Code: "sz000023"})
		if err != nil || resp == nil || resp.Count != 0 || len(resp.List) != 0 {
			t.Fatalf("empty response: resp=%+v err=%v", resp, err)
		}
	}
}

func historyTradeFixture() []byte {
	// 09:30, 5.85 CNY, 100 hands, status 0; then 09:31, -0.03 CNY, 1 hand, status 1.
	return []byte{2, 0, 0, 0, 0, 0, 0x3a, 2, 0x89, 9, 0xa4, 1, 0, 0, 0x3b, 2, 0x43, 1, 1, 0}
}

func TestHistoryTradeDecodeValid(t *testing.T) {
	resp, err := MHistoryTrade.Decode(historyTradeFixture(), TradeCache{Date: "20240122", Code: "sz000023"})
	if err != nil || resp == nil || len(resp.List) != 2 || resp.Count != 2 {
		t.Fatalf("valid response: resp=%+v err=%v", resp, err)
	}
	if resp.List[0].Price != 5850 || resp.List[1].Price != 5820 || resp.List[0].Volume != 100 || resp.List[1].Volume != 1 || resp.List[1].Status != 1 {
		t.Fatalf("changed price/volume/status units: %+v / %+v", resp.List[0], resp.List[1])
	}
	want := time.Date(2024, 1, 22, 9, 30, 0, 0, locationCSTHistory)
	if !resp.List[0].Time.Equal(want) {
		t.Fatalf("wrong exchange time: %v", resp.List[0].Time)
	}
}

func TestHistoryTradeDecodeTruncated(t *testing.T) {
	valid := historyTradeFixture()
	for end := 0; end < len(valid); end++ {
		t.Run(string(rune('A'+end)), func(t *testing.T) {
			resp, err := MHistoryTrade.Decode(valid[:end], TradeCache{Date: "20240122", Code: "sz000023"})
			if err == nil || resp != nil {
				t.Fatalf("truncated at %d returned partial success: resp=%+v err=%v", end, resp, err)
			}
		})
	}
}

func TestHistoryTradeDecodeMalformed(t *testing.T) {
	cases := [][]byte{
		{0, 0, 0}, // zero count does not legitimize a partial reserved header
		{1, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 1, 0, 0}, // invalid clock
		{1, 0, 0, 0, 0, 0, 0x3a, 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1, 0, 0},
	}
	for _, payload := range cases {
		resp, err := MHistoryTrade.Decode(payload, TradeCache{Date: "20240122", Code: "sz000023"})
		if err == nil || resp != nil {
			t.Fatalf("invalid response accepted: %x resp=%+v err=%v", payload, resp, err)
		}
	}
}
