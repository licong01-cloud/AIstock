package tdx

import (
	"github.com/injoyai/tdx/protocol"
	"testing"
	"time"
)

func TestPaginationCannotWrapOrDuplicate(t *testing.T) {
	calls := 0
	fetch := func(offset, count uint16) (*protocol.KlineResp, error) {
		calls++
		p := &protocol.KlineResp{Count: int(count)}
		for i := 0; i < int(count); i++ {
			p.List = append(p.List, &protocol.Kline{Time: time.Unix(100000-int64(offset)-int64(count)+int64(i), 0)})
		}
		return p, nil
	}
	if _, err := collectKlines(fetch, func(*protocol.Kline) bool { return false }); err == nil {
		t.Fatal("unbounded source accepted")
	}
	if calls != 82 {
		t.Fatalf("wrapped request, calls=%d", calls)
	}
	calls = 0
	repeated := func(offset, count uint16) (*protocol.KlineResp, error) { return fetch(0, count) }
	if _, err := collectKlines(repeated, func(*protocol.Kline) bool { return false }); err == nil || calls != 2 {
		t.Fatal("repeated source page accepted")
	}
}

func TestPaginationStopsAtRequestedBoundary(t *testing.T) {
	calls := 0
	p := &protocol.KlineResp{Count: 3, List: []*protocol.Kline{{Time: time.Unix(1, 0)}, {Time: time.Unix(2, 0)}, {Time: time.Unix(3, 0)}}}
	r, err := collectKlines(func(uint16, uint16) (*protocol.KlineResp, error) { calls++; return p, nil }, func(k *protocol.Kline) bool { return k.Time.Unix() <= 2 })
	if err != nil || calls != 1 || r.Count != 2 || r.List[0].Time.Unix() != 2 {
		t.Fatalf("boundary: %#v %v", r, err)
	}
}
