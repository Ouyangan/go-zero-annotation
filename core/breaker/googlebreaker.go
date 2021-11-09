package breaker

import (
	"math"
	"time"

	"github.com/tal-tech/go-zero/core/collection"
	"github.com/tal-tech/go-zero/core/mathx"
)

const (
	// 250ms for bucket duration
	window     = time.Second * 10
	buckets    = 40
	k          = 1.5
	protection = 5
)

// googleBreaker is a netflixBreaker pattern from google.
// see Client-Side Throttling section in https://landing.google.com/sre/sre-book/chapters/handling-overload/
type googleBreaker struct {
	//敏感度，默认值为1.5
	k float64
	//滑动窗口，用于记录最近一段时间内的请求总数，成功总数
	stat *collection.RollingWindow
	//概率生成器
	//随机产生0.0-1.0之间的双精度浮点数
	proba *mathx.Proba
}

//自适应熔断器构造器
func newGoogleBreaker() *googleBreaker {
	//滑动时间单元间隔时间
	//窗口时间/窗口数
	bucketDuration := time.Duration(int64(window) / int64(buckets))
	st := collection.NewRollingWindow(buckets, bucketDuration)
	return &googleBreaker{
		stat:  st,
		k:     k,
		proba: mathx.NewProba(),
	}
}

//按照最近一段时间的请求数据计算是否熔断
func (b *googleBreaker) accept() error {
	//获取最近一段时间的统计数据
	accepts, total := b.history()
	//计算动态熔断概率
	weightedAccepts := b.k * float64(accepts)
	// https://landing.google.com/sre/sre-book/chapters/handling-overload/#eq2101
	dropRatio := math.Max(0, (float64(total-protection)-weightedAccepts)/float64(total+1))
	//概率为0，通过
	if dropRatio <= 0 {
		return nil
	}
	//随机产生0.0-1.0之间的随机数与上面计算出来的熔断概率相比较
	//如果随机数比熔断概率小则进行熔断
	if b.proba.TrueOnProba(dropRatio) {
		return ErrServiceUnavailable
	}

	return nil
}

//熔断方法
//返回一个promise可有开发者自行决定是否上报结果到熔断器
func (b *googleBreaker) allow() (internalPromise, error) {
	if err := b.accept(); err != nil {
		return nil, err
	}

	return googlePromise{
		b: b,
	}, nil
}

//熔断方法
//req - 熔断对象方法
//fallback - 自定义快速失败函数，可对熔断产生的err进行包装后返回
//acceptable - 对本次未熔断时执行请求的结果进行自定义的判定，比如可以针对http.code,rpc.code,body.code
func (b *googleBreaker) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	//判定是否熔断
	if err := b.accept(); err != nil {
		//熔断中，如果有自定义的fallback则执行
		if fallback != nil {
			return fallback(err)
		}

		return err
	}
	//如果执行req()过程发生了panic，依然判定本次执行失败上报至熔断器
	defer func() {
		if e := recover(); e != nil {
			b.markFailure()
			panic(e)
		}
	}()
	//执行请求
	err := req()
	//判定请求成功
	if acceptable(err) {
		b.markSuccess()
	} else {
		b.markFailure()
	}

	return err
}

//上报成功
func (b *googleBreaker) markSuccess() {
	b.stat.Add(1)
}

//上报失败
func (b *googleBreaker) markFailure() {
	b.stat.Add(0)
}

//统计数据
func (b *googleBreaker) history() (accepts, total int64) {
	b.stat.Reduce(func(b *collection.Bucket) {
		accepts += int64(b.Sum)
		total += b.Count
	})

	return
}

type googlePromise struct {
	b *googleBreaker
}

func (p googlePromise) Accept() {
	p.b.markSuccess()
}

func (p googlePromise) Reject() {
	p.b.markFailure()
}
