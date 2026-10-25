package tdx

import (
	"fmt"
	"github.com/injoyai/tdx/protocol"
)

// The wire offset is uint16, the assembled fact count is not.
func collectKlines(fetch func(uint16, uint16) (*protocol.KlineResp, error), stop func(*protocol.Kline) bool) (*protocol.KlineResp, error) {
	out := &protocol.KlineResp{}
	const size = 800
	for offset := 0; ; offset += size {
		if offset > 65535 {
			return nil, fmt.Errorf("TDX history offset limit reached before source exhausted")
		}
		page, err := fetch(uint16(offset), size)
		if err != nil {
			return nil, err
		}
		if page == nil || page.Count != len(page.List) || len(page.List) > size {
			return nil, fmt.Errorf("invalid TDX page count at offset %d", offset)
		}
		for i, k := range page.List {
			if k == nil || (i > 0 && !page.List[i-1].Time.Before(k.Time)) {
				return nil, fmt.Errorf("unordered/duplicate TDX page at offset %d", offset)
			}
		}
		if len(out.List) > 0 && len(page.List) > 0 {
			if !page.List[len(page.List)-1].Time.Before(out.List[0].Time) {
				return nil, fmt.Errorf("TDX pagination made no progress at offset %d", offset)
			}
			out.List[0].Last = page.List[len(page.List)-1].Close
		}
		for i := len(page.List) - 1; i >= 0; i-- {
			if stop(page.List[i]) {
				out.List = append(page.List[i:], out.List...)
				out.Count = len(out.List)
				return out, nil
			}
		}
		out.List = append(page.List, out.List...)
		out.Count = len(out.List)
		if len(page.List) < size {
			return out, nil
		}
	}
}
