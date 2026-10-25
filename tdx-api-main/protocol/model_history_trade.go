package protocol

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/injoyai/conv"
)

var (
	// 中国标准时间时区 (UTC+8) - 用于历史分时成交
	locationCSTHistory = time.FixedZone("CST", 8*3600)
)

// HistoryTradeResp 兼容之前的版本
type HistoryTradeResp = TradeResp

type historyTrade struct{}

func (historyTrade) Frame(date, code string, start, count uint16) (*Frame, error) {
	exchange, number, err := DecodeCode(code)
	if err != nil {
		return nil, err
	}
	dataBs := Bytes(conv.Uint32(date)) //req.Time.Format("20060102"))
	dataBs = append(dataBs, exchange.Uint8(), 0x0)
	dataBs = append(dataBs, []byte(number)...)
	dataBs = append(dataBs, Bytes(start)...)
	dataBs = append(dataBs, Bytes(count)...)
	return &Frame{
		Control: Control01,
		Type:    TypeHistoryMinuteTrade,
		Data:    dataBs,
	}, nil
}

func (historyTrade) Decode(bs []byte, c TradeCache) (*TradeResp, error) {
	if len(bs) < 2 {
		return nil, errors.New("数据长度不足")
	}

	_, number, err := DecodeCode(c.Code)
	if err != nil {
		return nil, err
	}

	resp := &TradeResp{
		Count: Uint16(bs[:2]),
	}
	// The server uses a count-only response for an empty historical page.
	if resp.Count == 0 && len(bs) == 2 {
		return resp, nil
	}
	if len(bs) < 6 {
		return nil, errors.New("truncated historical trade header")
	}

	//第2-6字节不知道是啥
	bs = bs[2+4:]

	lastPrice := Price(0)
	for i := uint16(0); i < resp.Count; i++ {
		if len(bs) < 2 {
			return nil, fmt.Errorf("historical trade %d: truncated clock", i)
		}
		timeStr := GetHourMinute([2]byte(bs[:2]))
		// 数据中的时间本身就是北京时间，使用CST时区解析
		t, err := time.ParseInLocation("2006010215:04", c.Date+timeStr, locationCSTHistory)
		if err != nil {
			return nil, err
		}
		mt := &Trade{Time: t}
		var sub Price
		bs, sub, err = GetPriceChecked(bs[2:])
		if err != nil {
			return nil, fmt.Errorf("historical trade %d price: %w", i, err)
		}
		if sub > math.MaxInt64/10 || sub < math.MinInt64/10 {
			return nil, errors.New("historical trade price delta overflow")
		}
		delta := sub * 10 //把分转成厘
		if (delta > 0 && lastPrice > math.MaxInt64-delta) || (delta < 0 && lastPrice < math.MinInt64-delta) {
			return nil, errors.New("historical trade cumulative price overflow")
		}
		lastPrice += delta
		mt.Price = lastPrice / basePrice(number)
		var fields [3]Price
		for field := range fields {
			bs, fields[field], err = GetPriceChecked(bs)
			if err != nil {
				return nil, fmt.Errorf("historical trade %d field %d: %w", i, field, err)
			}
		}
		mt.Volume, mt.Status = int(fields[0]), int(fields[1])
		resp.List = append(resp.List, mt)
	}

	return resp, nil
}
