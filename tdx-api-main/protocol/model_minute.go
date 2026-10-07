package protocol

import (
	"errors"
	"fmt"
)

type MinuteResp struct {
	Count uint16
	List  []PriceNumber
}

type PriceNumber struct {
	Time   string
	Price  Price
	Number int
}

func (this PriceNumber) String() string {
	return fmt.Sprintf("%s \t%-6s \t%-6d(手)", this.Time, this.Price, this.Number)
}

type minute struct{}

func (this *minute) Frame(code string) (*Frame, error) {
	exchange, number, err := DecodeCode(code)
	if err != nil {
		return nil, err
	}
	codeBs := []byte(number)
	codeBs = append(codeBs, 0x0, 0x0, 0x0, 0x0)
	return &Frame{
		Control: Control01,
		Type:    TypeMinute,
		Data:    append([]byte{exchange.Uint8(), 0x0}, codeBs...),
	}, nil
}

func (this *minute) Decode(bs []byte) (*MinuteResp, error) {
	return decodeMinuteFacts(bs, 4)
}

// Intraday wire uses a four-byte header, historical wire uses six bytes.
func decodeMinuteFacts(bs []byte, header int) (*MinuteResp, error) {
	if len(bs) < header {
		return nil, errors.New("truncated minute header")
	}
	r := &MinuteResp{Count: Uint16(bs[:2])}
	bs = bs[header:]
	if r.Count > 240 {
		return nil, errors.New("minute count exceeds trading session")
	}
	var last Price
	for i := 0; i < int(r.Count); i++ {
		values := [3]Price{}
		for j := range values {
			var err error
			bs, values[j], err = GetPriceChecked(bs)
			if err != nil {
				return nil, fmt.Errorf("minute %d field %d: %w", i, j, err)
			}
		}
		last += values[0]
		if last <= 0 || values[2] < 0 {
			return nil, errors.New("invalid minute price/volume")
		}
		minutes := 9*60 + 30 + i + 1
		if i >= 120 {
			minutes += 90
		}
		r.List = append(r.List, PriceNumber{Time: fmt.Sprintf("%02d:%02d", minutes/60, minutes%60), Price: last * 10, Number: int(values[2])})
	}
	if len(bs) != 0 {
		return nil, errors.New("unexpected trailing minute bytes")
	}
	return r, nil
}
