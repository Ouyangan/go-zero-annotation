package breaker

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/tal-tech/go-zero/core/mathx"
	"github.com/tal-tech/go-zero/core/proc"
	"github.com/tal-tech/go-zero/core/stat"
	"github.com/tal-tech/go-zero/core/stringx"
	"github.com/tal-tech/go-zero/core/timex"
)

const (
	numHistoryReasons = 5
	timeFormat        = "15:04:05"
)

// ErrServiceUnavailable is returned when the Breaker state is open.
var ErrServiceUnavailable = errors.New("circuit breaker is open")

type (
	// Acceptable is the func to check if the error can be accepted.
	//自定义判定执行结果
	Acceptable func(err error) bool

	// A Breaker represents a circuit breaker.
	Breaker interface {
		// Name returns the name of the Breaker.
		//熔断器名称
		Name() string

		// Allow checks if the request is allowed.
		// If allowed, a promise will be returned, the caller needs to call promise.Accept()
		// on success, or call promise.Reject() on failure.
		// If not allow, ErrServiceUnavailable will be returned.
		//熔断方法，执行请求时必须手动上报执行结果
		//适用于简单无需自定义快速失败，无需自定义判定请求结果的场景
		Allow() (Promise, error)

		// Do runs the given request if the Breaker accepts it.
		// Do returns an error instantly if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		//熔断方法，自动上报执行结果
		Do(req func() error) error

		// DoWithAcceptable runs the given request if the Breaker accepts it.
		// DoWithAcceptable returns an error instantly if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		// acceptable checks if it's a successful call, even if the err is not nil.
		//熔断方法
		//acceptable - 支持自定义判定执行结果
		DoWithAcceptable(req func() error, acceptable Acceptable) error

		// DoWithFallback runs the given request if the Breaker accepts it.
		// DoWithFallback runs the fallback if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		//熔断方法
		//fallback - 支持自定义快速失败
		DoWithFallback(req func() error, fallback func(err error) error) error

		// DoWithFallbackAcceptable runs the given request if the Breaker accepts it.
		// DoWithFallbackAcceptable runs the fallback if the Breaker rejects the request.
		// If a panic occurs in the request, the Breaker handles it as an error
		// and causes the same panic again.
		// acceptable checks if it's a successful call, even if the err is not nil.
		//熔断方法
		//fallback - 支持自定义快速失败
		//acceptable - 支持自定义判定执行结果
		DoWithFallbackAcceptable(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	// Option defines the method to customize a Breaker.
	Option func(breaker *circuitBreaker)

	// Promise interface defines the callbacks that returned by Breaker.Allow.
	//手动回调
	Promise interface {
		// Accept tells the Breaker that the call is successful.
		//请求成功
		Accept()
		// Reject tells the Breaker that the call is failed.
		//请求失败
		Reject(reason string)
	}

	//熔断器结构体
	circuitBreaker struct {
		name string
		//实际上 circuitBreaker熔断功能都代理给 throttle来实现
		throttle
	}
	//熔断器接口
	throttle interface {
		//熔断方法
		allow() (Promise, error)
		//熔断方法
		//DoXXX()方法最终都会该方法
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}
	//熔断器接口
	internalThrottle interface {
		allow() (internalPromise, error)
		doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error
	}

	//回调接口
	internalPromise interface {
		Accept()
		Reject()
	}
)

// NewBreaker returns a Breaker object.
// opts can be used to customize the Breaker.
func NewBreaker(opts ...Option) Breaker {
	var b circuitBreaker
	for _, opt := range opts {
		opt(&b)
	}
	//未设置名称则默认随机生成一个
	if len(b.name) == 0 {
		b.name = stringx.Rand()
	}
	b.throttle = newLoggedThrottle(b.name, newGoogleBreaker())

	return &b
}

func (cb *circuitBreaker) Allow() (Promise, error) {
	return cb.throttle.allow()
}

func (cb *circuitBreaker) Do(req func() error) error {
	return cb.throttle.doReq(req, nil, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithAcceptable(req func() error, acceptable Acceptable) error {
	return cb.throttle.doReq(req, nil, acceptable)
}

func (cb *circuitBreaker) DoWithFallback(req func() error, fallback func(err error) error) error {
	return cb.throttle.doReq(req, fallback, defaultAcceptable)
}

func (cb *circuitBreaker) DoWithFallbackAcceptable(req func() error, fallback func(err error) error,
	acceptable Acceptable) error {
	return cb.throttle.doReq(req, fallback, acceptable)
}

func (cb *circuitBreaker) Name() string {
	return cb.name
}

// WithName returns a function to set the name of a Breaker.
func WithName(name string) Option {
	return func(b *circuitBreaker) {
		b.name = name
	}
}

func defaultAcceptable(err error) bool {
	return err == nil
}

//带日志功能的熔断器
type loggedThrottle struct {
	//名称
	name string
	//代理对象
	internalThrottle
	//滚动窗口,滚动收集数据,相当于环形数组
	errWin *errorWindow
}

func newLoggedThrottle(name string, t internalThrottle) loggedThrottle {
	return loggedThrottle{
		name:             name,
		internalThrottle: t,
		errWin:           new(errorWindow),
	}
}

//熔断方法
func (lt loggedThrottle) allow() (Promise, error) {
	promise, err := lt.internalThrottle.allow()
	return promiseWithReason{
		promise: promise,
		errWin:  lt.errWin,
	}, lt.logError(err)
}

//熔断方法
func (lt loggedThrottle) doReq(req func() error, fallback func(err error) error, acceptable Acceptable) error {
	return lt.logError(lt.internalThrottle.doReq(req, fallback, func(err error) bool {
		accept := acceptable(err)
		if !accept {
			lt.errWin.add(err.Error())
		}
		return accept
	}))
}

func (lt loggedThrottle) logError(err error) error {
	if err == ErrServiceUnavailable {
		// if circuit open, not possible to have empty error window
		stat.Report(fmt.Sprintf(
			"proc(%s/%d), callee: %s, breaker is open and requests dropped\nlast errors:\n%s",
			proc.ProcessName(), proc.Pid(), lt.name, lt.errWin))
	}

	return err
}

//滚动窗口
type errorWindow struct {
	reasons [numHistoryReasons]string
	index   int
	count   int
	lock    sync.Mutex
}

//添加数据
func (ew *errorWindow) add(reason string) {
	ew.lock.Lock()
	//添加错误日志
	ew.reasons[ew.index] = fmt.Sprintf("%s %s", timex.Time().Format(timeFormat), reason)
	//更新index,为下一次写入数据做准备
	//这里用的取模实现了滚动功能
	ew.index = (ew.index + 1) % numHistoryReasons
	//统计数量
	ew.count = mathx.MinInt(ew.count+1, numHistoryReasons)
	ew.lock.Unlock()
}

//格式化错误日志
func (ew *errorWindow) String() string {
	var reasons []string

	ew.lock.Lock()
	// reverse order
	for i := ew.index - 1; i >= ew.index-ew.count; i-- {
		reasons = append(reasons, ew.reasons[(i+numHistoryReasons)%numHistoryReasons])
	}
	ew.lock.Unlock()

	return strings.Join(reasons, "\n")
}

type promiseWithReason struct {
	promise internalPromise
	errWin  *errorWindow
}

func (p promiseWithReason) Accept() {
	p.promise.Accept()
}

func (p promiseWithReason) Reject(reason string) {
	p.errWin.add(reason)
	p.promise.Reject()
}
