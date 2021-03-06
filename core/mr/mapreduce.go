package mr

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tal-tech/go-zero/core/errorx"
	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/syncx"
	"github.com/tal-tech/go-zero/core/threading"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var (
	// ErrCancelWithNil is an error that mapreduce was cancelled with nil.
	ErrCancelWithNil = errors.New("mapreduce cancelled with nil")
	// ErrReduceNoOutput is an error that reduce did not output a value.
	ErrReduceNoOutput = errors.New("reduce not writing value")
)

type (
	// GenerateFunc is used to let callers send elements into source.
	//数据生产func
	//source - 数据被生产后写入source
	GenerateFunc func(source chan<- interface{})
	// MapFunc is used to do element processing and write the output to writer.
	MapFunc func(item interface{}, writer Writer)
	// VoidMapFunc is used to do element processing, but no output.
	VoidMapFunc func(item interface{})
	// MapperFunc is used to do element processing and write the output to writer,
	// use cancel func to cancel the processing.
	//数据加工func
	//item - 生产出来的数据
	//writer - 调用writer.Write()可以将加工后的向后传递至reducer
	//cancel - 终止流程func
	MapperFunc func(item interface{}, writer Writer, cancel func(error))
	// ReducerFunc is used to reduce all the mapping output and write to writer,
	// use cancel func to cancel the processing.
	//数据聚合func
	//pipe - 加工出来的数据
	//writer - 调用writer.Write()可以将聚合后的数据返回给用户
	//cancel - 终止流程func
	ReducerFunc func(pipe <-chan interface{}, writer Writer, cancel func(error))
	// VoidReducerFunc is used to reduce all the mapping output, but no output.
	// Use cancel func to cancel the processing.
	VoidReducerFunc func(pipe <-chan interface{}, cancel func(error))
	// Option defines the method to customize the mapreduce.
	Option func(opts *mapReduceOptions)

	mapReduceOptions struct {
		workers int
	}

	// Writer interface wraps Write method.
	Writer interface {
		Write(v interface{})
	}
)

// Finish runs fns parallelly, cancelled on any error.
//并发执行func，发生任何错误将会立即终止流程
func Finish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, writer Writer, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, func(pipe <-chan interface{}, cancel func(error)) {
		drain(pipe)
	}, WithWorkers(len(fns)))
}

// FinishVoid runs fns parallelly.
//并发执行func，即使发生错误也不会终止流程
func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	MapVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	}, WithWorkers(len(fns)))
}

// Map maps all elements generated from given generate func, and returns an output channel.
//需要用户手动将生产数据写入 source，加工数据后返回一个channel供读取
//opts - 可选参数，目前包含：数据加工阶段协程数量
func Map(generate GenerateFunc, mapper MapFunc, opts ...Option) chan interface{} {
	options := buildOptions(opts...)
	source := buildSource(generate)
	collector := make(chan interface{}, options.workers)
	done := syncx.NewDoneChan()

	go executeMappers(mapper, source, collector, done.Done(), options.workers)

	return collector
}

// MapReduce maps all elements generated from given generate func,
// and reduces the output elements with given reducer.
//需要用户手动将生产数据写入 source ，并返回聚合后的数据
//generate 生产
//mapper 加工
//reducer 聚合
//opts - 可选参数，目前包含：数据加工阶段协程数量
func MapReduce(generate GenerateFunc, mapper MapperFunc, reducer ReducerFunc, opts ...Option) (interface{}, error) {
	source := buildSource(generate)
	return MapReduceWithSource(source, mapper, reducer, opts...)
}

// MapReduceWithSource maps all elements from source, and reduce the output elements with given reducer.
//支持传入数据源channel，并返回聚合后的数据
//source - 数据源channel
//mapper - 读取source内容并处理
//reducer - 数据处理完毕发送至reducer聚合
func MapReduceWithSource(source <-chan interface{}, mapper MapperFunc, reducer ReducerFunc,
	opts ...Option) (interface{}, error) {
	//可选参数设置
	options := buildOptions(opts...)
	//聚合数据channel，需要手动调用write方法写入到output中
	output := make(chan interface{})
	//output最后只会被读取一次
	defer func() {
		//如果有多次写入的话则会造成阻塞从而导致协程泄漏
		//这里用 for range检测是否可以读出数据，读出数据说明多次写入了
		//为什么这里使用panic呢？显示的提醒用户用法错了会比自动修复掉好一些
		for range output {
			panic("more than one element written in reducer")
		}
	}()
	//创建有缓冲的chan，容量为workers
	//意味着最多允许 workers 个协程同时处理数据
	collector := make(chan interface{}, options.workers)
	//数据聚合任务完成标志
	done := syncx.NewDoneChan()
	//支持阻塞写入chan的writer
	writer := newGuardedWriter(output, done.Done())
	//单例关闭
	var closeOnce sync.Once
	var retErr errorx.AtomicError
	//数据聚合任务已结束，发送完成标志
	finish := func() {
		//只能关闭一次
		closeOnce.Do(func() {
			//发送聚合任务完成信号，close函数将会向chan写入一个零值
			done.Close()
			//关闭数据聚合chan
			close(output)
		})
	}
	//取消操作
	cancel := once(func(err error) {
		//设置error
		if err != nil {
			retErr.Set(err)
		} else {
			retErr.Set(ErrCancelWithNil)
		}
		//清空source channel
		drain(source)
		//调用完成方法
		finish()
	})

	go func() {
		defer func() {
			//清空聚合任务channel
			drain(collector)
			//捕获panic
			if r := recover(); r != nil {
				//调用cancel方法，立即结束
				cancel(fmt.Errorf("%v", r))
			} else {
				//正常结束
				finish()
			}
		}()
		//执行数据加工
		//注意writer.write将加工后数据写入了output
		reducer(collector, writer, cancel)
	}()
	//异步执行数据加工
	//source - 数据生产
	//collector - 数据收集
	//done - 结束标志
	//workers - 并发数
	go executeMappers(func(item interface{}, w Writer) {
		mapper(item, w, cancel)
	}, source, collector, done.Done(), options.workers)
	//reducer将加工后的数据写入了output，
	//需要数据返回时读取output即可
	//假如output被写入了超过两次
	//则开始的defer func那里将还可以读到数据
	//由此可以检测到用户调用了多次write方法
	value, ok := <-output
	if err := retErr.Load(); err != nil {
		return nil, err
	} else if ok {
		return value, nil
	} else {
		return nil, ErrReduceNoOutput
	}
}

// MapReduceVoid maps all elements generated from given generate,
// and reduce the output elements with given reducer.
//无返回值，关注错误
func MapReduceVoid(generate GenerateFunc, mapper MapperFunc, reducer VoidReducerFunc, opts ...Option) error {
	_, err := MapReduce(generate, mapper, func(input <-chan interface{}, writer Writer, cancel func(error)) {
		reducer(input, cancel)
		// We need to write a placeholder to let MapReduce to continue on reducer done,
		// otherwise, all goroutines are waiting. The placeholder will be discarded by MapReduce.
		writer.Write(lang.Placeholder)
	}, opts...)
	return err
}

// MapVoid maps all elements from given generate but no output.
//无返回值，不关注错误
func MapVoid(generate GenerateFunc, mapper VoidMapFunc, opts ...Option) {
	drain(Map(generate, func(item interface{}, writer Writer) {
		mapper(item)
	}, opts...))
}

// WithWorkers customizes a mapreduce processing with given workers.
func WithWorkers(workers int) Option {
	return func(opts *mapReduceOptions) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

func buildOptions(opts ...Option) *mapReduceOptions {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

//数据生产
func buildSource(generate GenerateFunc) chan interface{} {
	source := make(chan interface{})
	//防止数据生产时产生panic错误，因此需要recover掉
	//异步的生产数据
	threading.GoSafe(func() {
		//数据生产完毕则关闭chan
		defer close(source)
		generate(source)
	})
	//返回chan供数据处理mapper读取
	return source
}

// drain drains the channel.
//清空 channel
func drain(channel <-chan interface{}) {
	// drain the channel
	//channel关闭后，for喜欢将会自动跳出
	for range channel {
	}
}

//数据加工
func executeMappers(mapper MapFunc, input <-chan interface{}, collector chan<- interface{},
	done <-chan lang.PlaceholderType, workers int) {
	//goroutine协调同步信号量
	var wg sync.WaitGroup
	defer func() {
		//等待数据加工任务完成
		//防止数据加工的协程还未处理完数据就直接退出了
		wg.Wait()
		//关闭数据加工channel
		close(collector)
	}()
	//带缓冲区的channel，缓冲区大小为workers
	//控制数据加工的协程数量
	pool := make(chan lang.PlaceholderType, workers)
	//数据加工writer
	writer := newGuardedWriter(collector, done)
	for {
		select {
		//监听到外部结束信号，直接结束
		case <-done:
			return
		//控制数据加工协程数量
		//缓冲区容量-1
		//无容量时将会被阻塞，等待释放容量
		case pool <- lang.Placeholder:
			//阻塞等待生产数据channel
			item, ok := <-input
			//如果ok为false则说明input已被关闭或者清空
			//数据加工完成，执行退出
			if !ok {
				//缓冲区容量+1
				<-pool
				//结束本次循环
				return
			}
			//wg同步信号量+1
			wg.Add(1)
			// better to safely run caller defined method
			//异步执行数据加工，防止panic错误
			threading.GoSafe(func() {
				defer func() {
					//wg同步信号量-1
					wg.Done()
					//缓冲区容量+1
					<-pool
				}()

				mapper(item, writer)
			})
		}
	}
}

func newOptions() *mapReduceOptions {
	return &mapReduceOptions{
		workers: defaultWorkers,
	}
}

func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

type guardedWriter struct {
	//写入chan
	channel chan<- interface{}
	//完成标志
	done <-chan lang.PlaceholderType
}

func newGuardedWriter(channel chan<- interface{}, done <-chan lang.PlaceholderType) guardedWriter {
	return guardedWriter{
		channel: channel,
		done:    done,
	}
}

//阻塞等待数据写入
//这里需要特别注意
//对于gw而言如果有多个地方调用Write方法的话其中一个会永远阻塞导致协程泄漏
func (gw guardedWriter) Write(v interface{}) {
	select {
	//任务已完成，结束阻塞
	//close信号也是可以接受到的
	case <-gw.done:
		return
	//继续写入数据
	default:
		gw.channel <- v
	}
}
