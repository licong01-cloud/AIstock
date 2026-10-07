package protocol

import (
	"errors"
	"fmt"
	"github.com/injoyai/base/types"
	"github.com/injoyai/conv"
	"math"
	"sort"
	"time"
)

type KlineReq struct {
	Exchange Exchange
	Code     string
	Start    uint16
	Count    uint16
}

func (this *KlineReq) Bytes(Type uint8) (types.Bytes, error) {
	if this.Count > 800 {
		return nil, errors.New("单次数量不能超过800")
	}
	if len(this.Code) != 6 {
		return nil, errors.New("股票代码长度错误")
	}
	data := []byte{this.Exchange.Uint8(), 0x0}
	data = append(data, []byte(this.Code)...) //这里怎么是正序了？
	data = append(data, Type, 0x0)
	data = append(data, 0x01, 0x0)
	data = append(data, Bytes(this.Start)...)
	data = append(data, Bytes(this.Count)...)
	data = append(data, make([]byte, 10)...) //未知啥含义
	return data, nil
}

type KlineResp struct {
	Count int
	List  []*Kline
}

type Kline struct {
	Last         Price     //昨日收盘价,这个是列表的上一条数据的收盘价，如果没有上条数据，那么这个值为0
	Open         Price     //开盘价
	High         Price     //最高价
	Low          Price     //最低价
	Close        Price     //收盘价,如果是当天,则是最新价/实时价
	Volume       int64     //成交量
	VolumeShares *int64    `json:"VolumeShares,omitempty"` // Exact stock shares; legacy Volume remains whole hands.
	VolumeWire   uint32    `json:"VolumeWire"`
	AmountWire   uint32    `json:"AmountWire"`
	Amount       Price     //成交额
	Time         time.Time //时间
	UpCount      int       //上涨数量,指数有效
	DownCount    int       //下跌数量,指数有效
}

func (this *Kline) String() string {
	return fmt.Sprintf("%s 昨收盘：%.3f 开盘价：%.3f 最高价：%.3f 最低价：%.3f 收盘价：%.3f 涨跌：%s 涨跌幅：%0.2f 成交量：%s 成交额：%s 涨跌数: %d/%d",
		this.Time.Format("2006-01-02 15:04:05"),
		this.Last.Float64(), this.Open.Float64(), this.High.Float64(), this.Low.Float64(), this.Close.Float64(),
		this.RisePrice(), this.RiseRate(),
		Int64UnitString(this.Volume), FloatUnitString(this.Amount.Float64()),
		this.UpCount, this.DownCount,
	)
}

// MaxDifference 最大差值，最高-最低
func (this *Kline) MaxDifference() Price {
	return this.High - this.Low
}

// RisePrice 涨跌金额,第一个数据不准，仅做参考
func (this *Kline) RisePrice() Price {
	if this.Last == 0 {
		//稍微数据准确点，没减去0这么夸张，还是不准的
		return this.Close - this.Open
	}
	return this.Close - this.Last

}

// RiseRate 涨跌比例/涨跌幅,第一个数据不准，仅做参考
func (this *Kline) RiseRate() float64 {
	if this.Last == 0 {
		return float64(this.Close-this.Open) / float64(this.Open) * 100
	}
	return float64(this.Close-this.Last) / float64(this.Last) * 100
}

type kline struct{}

/*
Frame
Prefix: 0c
MsgID: 0208d301
Control: 01
Length: 1c00
Length: 1c00
Type: 2d05
Data: 000030303030303104000100a401a40100000000000000000000

Data:
Exchange: 00
Unknown: 00
Code: 303030303031
Type: 04
Unknown: 00
Unknown: 0100
Start: a401
Count: a401
Append: 00000000000000000000
*/
func (kline) Frame(Type uint8, code string, start, count uint16) (*Frame, error) {
	if count > 800 {
		return nil, errors.New("单次数量不能超过800")
	}

	exchange, number, err := DecodeCode(code)
	if err != nil {
		return nil, err
	}

	data := []byte{exchange.Uint8(), 0x0}
	data = append(data, []byte(number)...) //这里怎么是正序了？
	data = append(data, Type, 0x0)
	data = append(data, 0x01, 0x0)
	data = append(data, Bytes(start)...)
	data = append(data, Bytes(count)...)
	data = append(data, make([]byte, 10)...) //未知啥含义

	return &Frame{
		Control: Control01,
		Type:    TypeKline,
		Data:    data,
	}, nil
}

func (kline) Decode(bs []byte, c KlineCache) (*KlineResp, error) {

	if len(bs) < 2 {
		return nil, errors.New("数据长度不足")
	}
	resp := &KlineResp{
		Count: int(Uint16(bs[:2])),
	}
	bs = bs[2:]

	var last Price //上条数据(昨天)的收盘价
	for i := 0; i < resp.Count; i++ {
		if len(bs) < 4 {
			return nil, fmt.Errorf("kline %d: truncated timestamp", i)
		}
		k := &Kline{
			Time: GetTime([4]byte(bs[:4]), c.Type),
		}
		if !validWireTime(bs[:4], c.Type, k.Time) {
			return nil, fmt.Errorf("kline %d: invalid exchange timestamp", i)
		}

		bs = bs[4:]
		values := [4]Price{}
		for j := range values {
			var err error
			bs, values[j], err = GetPriceChecked(bs)
			if err != nil {
				return nil, fmt.Errorf("kline %d price %d: %w", i, j, err)
			}
		}
		open, _close, high, low := values[0], values[1], values[2], values[3]

		k.Last = last
		k.Open = open + last
		k.Close = last + open + _close
		k.High = open + last + high
		k.Low = open + last + low
		last = last + open + _close

		/*
			发现不同的K线数据处理不一致,测试如下:
			1分: 需要除以100
			5分: 需要除以100
			15分: 需要除以100
			30分: 需要除以100
			60分: 需要除以100
			日: 不需要操作
			周: 不需要操作
			月: 不需要操作
			季: 不需要操作
			年: 不需要操作

		*/
		if len(bs) < 8 {
			return nil, fmt.Errorf("kline %d: truncated volume/amount", i)
		}
		k.VolumeWire = Uint32(bs[:4])
		k.AmountWire = Uint32(bs[4:8])
		volume := getVolume(k.VolumeWire)
		amount := getVolume(k.AmountWire)
		if k.VolumeWire&0x80000000 != 0 || k.AmountWire&0x80000000 != 0 || !isFinite(volume) || !isFinite(amount) || volume >= math.MaxInt64 || amount >= float64(math.MaxInt64)/1000 {
			return nil, fmt.Errorf("kline %d: invalid packed quantity/amount", i)
		}
		k.Volume = int64(volume)
		bs = bs[4:]
		switch c.Type {
		case TypeKlineMinute, TypeKline5Minute, TypeKlineMinute2, TypeKline15Minute, TypeKline30Minute, TypeKline60Minute, TypeKlineDay2:
			if c.Kind == KindStock && !(volume == 0 && amount > 0) {
				shares := int64(volume)
				k.VolumeShares = &shares
			}
			k.Volume /= 100
		}
		k.Amount = Price(amount * 1000) // Wire amount is CNY, stored Price is li.
		bs = bs[4:]

		switch c.Kind {
		case KindIndex:
			if len(bs) < 4 {
				return nil, fmt.Errorf("kline %d: truncated index counts", i)
			}
			//指数和股票的差别,指数多解析4字节,并处理成交量*100
			k.Volume *= 100
			k.UpCount = conv.Int([]byte{bs[1], bs[0]})
			k.DownCount = conv.Int([]byte{bs[3], bs[2]})
			bs = bs[4:]
		}

		resp.List = append(resp.List, k)
	}
	if len(bs) != 0 {
		return nil, errors.New("unexpected trailing kline bytes")
	}
	return resp, nil
}

func isFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validWireTime(bs []byte, typ uint8, t time.Time) bool {
	switch typ {
	case TypeKlineMinute, TypeKlineMinute2, TypeKline5Minute, TypeKline15Minute, TypeKline30Minute, TypeKline60Minute:
		d, m := Uint16(bs[:2]), Uint16(bs[2:4])
		return m < 1440 && t.Year() == int(d>>11)+2004 && int(t.Month()) == int(d%2048/100) && t.Day() == int(d%2048%100)
	default:
		d := Uint32(bs)
		return t.Year() == int(d/10000) && int(t.Month()) == int(d%10000/100) && t.Day() == int(d%100)
	}
}

var ExchangeLocation = time.FixedZone("Asia/Shanghai", 8*3600)

type KlineCache struct {
	Type uint8  //1分钟,5分钟,日线等
	Kind string //指数,个股等
}

// FixKlineTime 修复盘内下午(13~15点)拉取数据的时候,11.30的时间变成13.00
// Deprecated: exchange wire timestamps are retained. A 13:00 anomaly must
// be resolved from source evidence, never from the process wall clock.
func FixKlineTime(ks []*Kline) []*Kline { return ks }

type Klines []*Kline

// LastPrice 获取最后一个K线的收盘价
func (this Klines) LastPrice() Price {
	if len(this) == 0 {
		return 0
	}
	return this[len(this)-1].Close
}

func (this Klines) Len() int {
	return len(this)
}

func (this Klines) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this Klines) Less(i, j int) bool {
	return this[i].Time.Before(this[j].Time)
}

func (this Klines) Sort() {
	sort.Sort(this)
}

func (this Klines) Kline(t time.Time, last Price) *Kline {
	k := &Kline{
		Time:   t,
		Open:   last,
		High:   last,
		Low:    last,
		Close:  last,
		Volume: 0,
		Amount: 0,
	}
	for i, v := range this {
		switch i {
		case 0:
			k.Open = v.Open
			k.High = v.High
			k.Low = v.Low
			k.Close = v.Close
		default:
			if k.Open == 0 {
				k.Open = v.Open
			}
			k.High = conv.Select(k.High < v.High, v.High, k.High)
			k.Low = conv.Select(k.Low > v.Low, v.Low, k.Low)
		}
		k.Close = v.Close
		k.Volume += v.Volume
		k.Amount += v.Amount
	}
	return k
}

// Merge 合并成其他类型的K线
func (this Klines) Merge(n int) Klines {
	if n <= 1 {
		return this
	}

	ks := Klines(nil)
	ls := Klines(nil)
	for i := 0; ; i++ {
		if len(this) <= i*n {
			break
		}
		if len(this) < (i+1)*n {
			ls = this[i*n:]
		} else {
			ls = this[i*n : (i+1)*n]
		}
		if len(ls) == 0 {
			break
		}
		last := ls[len(ls)-1]
		k := ls.Kline(last.Time, ls[0].Open)
		ks = append(ks, k)
	}
	return ks
}

//// Kline 计算多个K线,成一个K线
//func (this Klines) Kline() *Kline {
//	if this == nil {
//		return new(Kline)
//	}
//	k := new(Kline)
//	for i, v := range this {
//		switch i {
//		case 0:
//			k.Open = v.Open
//			k.High = v.High
//			k.Low = v.Low
//			k.Close = v.Close
//		case len(this) - 1:
//			k.Close = v.Close
//			k.Time = v.Time
//		}
//		if v.High > k.High {
//			k.High = v.High
//		}
//		if v.Low < k.Low {
//			k.Low = v.Low
//		}
//		k.Volume += v.Volume
//		k.Amount += v.Amount
//	}
//	return k
//}

//// Merge 合并K线,1分钟转成5,15,30分钟等
//func (this Klines) Merge(n int) Klines {
//	if this == nil {
//		return nil
//	}
//	ks := []*Kline(nil)
//	for i := 0; i < len(this); i += n {
//		if i+n > len(this) {
//			ks = append(ks, this[i:].Kline())
//		} else {
//			ks = append(ks, this[i:i+n].Kline())
//		}
//	}
//	return ks
//}
