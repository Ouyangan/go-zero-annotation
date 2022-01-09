package executors

import (
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/proc"
	"github.com/tal-tech/go-zero/core/syncx"
	"github.com/tal-tech/go-zero/core/threading"
	"github.com/tal-tech/go-zero/core/timex"
)

const idleRound = 10

type (
	// TaskContainer interface defines a type that can be used as the underlying
	// container that used to do periodical executions.
	// 任务容器
	TaskContainer interface {
		// AddTask adds the task into the container.
		// Returns true if the container needs to be flushed after the addition.
		// 添加任务
		AddTask(task interface{}) bool
		// Execute handles the collected tasks by the container when flushing.
		// 执行任务
		Execute(tasks interface{})
		// RemoveAll removes the contained tasks, and return them.
		// 移除全部任务
		RemoveAll() interface{}
	}

	// A PeriodicalExecutor is an executor that periodically execute tasks.
	// 周期任务执行器
	PeriodicalExecutor struct {
		// todo
		commander chan interface{}
		// 执行间隔
		interval time.Duration
		// 任务容器
		container TaskContainer
		// 同步器
		waitGroup sync.WaitGroup
		// avoid race condition on waitGroup when calling wg.Add/Done/Wait(...)
		// 同步器屏障 todo
		wgBarrier syncx.Barrier
		// todo
		confirmChan chan lang.PlaceholderType
		// 当前待处理的任务数量
		inflight int32
		// 监视器
		// todo 有什么作用呢？
		guarded bool
		// todo 定时器
		newTicker func(duration time.Duration) timex.Ticker
		// 独占锁
		lock sync.Mutex
	}
)

// NewPeriodicalExecutor returns a PeriodicalExecutor with given interval and container.
func NewPeriodicalExecutor(interval time.Duration, container TaskContainer) *PeriodicalExecutor {
	executor := &PeriodicalExecutor{
		// buffer 1 to let the caller go quickly
		// 带缓冲的channel，为了使用调用方更快的返回
		commander: make(chan interface{}, 1),
		//调度间隔时间
		interval: interval,
		//任务容器
		container: container,
		//todo ？
		confirmChan: make(chan lang.PlaceholderType),
		//定时器
		newTicker: func(d time.Duration) timex.Ticker {
			return timex.NewTicker(d)
		},
	}
	//退出信号监听，退出前执行任务
	proc.AddShutdownListener(func() {
		executor.Flush()
	})

	return executor
}

// Add adds tasks into pe.
// 添加任务
func (pe *PeriodicalExecutor) Add(task interface{}) {
	//
	if vals, ok := pe.addAndCheck(task); ok {

		pe.commander <- vals
		<-pe.confirmChan
	}
}

// Flush forces pe to execute tasks.
// 强制执行任务池中所有任务
func (pe *PeriodicalExecutor) Flush() bool {

	pe.enterExecution()
	return pe.executeTasks(func() interface{} {
		pe.lock.Lock()
		defer pe.lock.Unlock()
		return pe.container.RemoveAll()
	}())
}

// Sync lets caller to run fn thread-safe with pe, especially for the underlying container.
func (pe *PeriodicalExecutor) Sync(fn func()) {
	pe.lock.Lock()
	defer pe.lock.Unlock()
	fn()
}

// Wait waits the execution to be done.
//
func (pe *PeriodicalExecutor) Wait() {
	pe.Flush()
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Wait()
	})
}

// 添加任务到任务池
func (pe *PeriodicalExecutor) addAndCheck(task interface{}) (interface{}, bool) {
	//加锁
	pe.lock.Lock()
	defer func() {
		// 判断是否在
		if !pe.guarded {
			pe.guarded = true
			// defer to unlock quickly
			defer pe.backgroundFlush()
		}
		pe.lock.Unlock()
	}()

	// 任务添加成功
	if pe.container.AddTask(task) {
		// 处理中任务数量+1
		atomic.AddInt32(&pe.inflight, 1)
		// 为什么添加成功要获取所有任务呢？
		return pe.container.RemoveAll(), true
	}

	return nil, false
}

// 兜底任务策略
// 定时刷新任务
func (pe *PeriodicalExecutor) backgroundFlush() {
	threading.GoSafe(func() {
		// flush before quit goroutine to avoid missing tasks
		// todo 为什么会存在丢失任务的场景呢？
		defer pe.Flush()
		// 启动定时器
		ticker := pe.newTicker(pe.interval)
		defer ticker.Stop()

		// 运行标志
		var commanded bool

		last := timex.Now()
		for {
			select {
			case vals := <-pe.commander:
				commanded = true
				atomic.AddInt32(&pe.inflight, -1)
				pe.enterExecution()
				pe.confirmChan <- lang.Placeholder
				pe.executeTasks(vals)
				last = timex.Now()
			case <-ticker.Chan():
				if commanded {
					commanded = false
				} else if pe.Flush() {
					last = timex.Now()
				} else if pe.shallQuit(last) {
					return
				}
			}
		}
	})
}

func (pe *PeriodicalExecutor) doneExecution() {
	pe.waitGroup.Done()
}

func (pe *PeriodicalExecutor) enterExecution() {
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Add(1)
	})
}

func (pe *PeriodicalExecutor) executeTasks(tasks interface{}) bool {
	defer pe.doneExecution()

	ok := pe.hasTasks(tasks)
	if ok {
		pe.container.Execute(tasks)
	}

	return ok
}

func (pe *PeriodicalExecutor) hasTasks(tasks interface{}) bool {
	if tasks == nil {
		return false
	}

	val := reflect.ValueOf(tasks)
	switch val.Kind() {
	case reflect.Array, reflect.Chan, reflect.Map, reflect.Slice:
		return val.Len() > 0
	default:
		// unknown type, let caller execute it
		return true
	}
}

//
func (pe *PeriodicalExecutor) shallQuit(last time.Duration) (stop bool) {
	if timex.Since(last) <= pe.interval*idleRound {
		return
	}

	// checking pe.inflight and setting pe.guarded should be locked together
	pe.lock.Lock()
	if atomic.LoadInt32(&pe.inflight) == 0 {
		pe.guarded = false
		stop = true
	}
	pe.lock.Unlock()

	return
}
