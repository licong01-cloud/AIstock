package protocol

import (
	"github.com/injoyai/conv"
)

type historyMinute struct{}

func (this historyMinute) Frame(date, code string) (*Frame, error) {
	exchange, number, err := DecodeCode(code)
	if err != nil {
		return nil, err
	}
	dataBs := Bytes(conv.Uint32(date))
	dataBs = append(dataBs, exchange.Uint8())
	dataBs = append(dataBs, []byte(number)...)
	return &Frame{
		Control: Control01,
		Type:    TypeHistoryMinute,
		Data:    dataBs,
	}, nil
}

func (this historyMinute) Decode(bs []byte) (*MinuteResp, error) {
	return decodeMinuteFacts(bs, 6)
}
