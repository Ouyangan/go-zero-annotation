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
		// todo 共享调用?
		inflight int32
		// 监视器
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
		// 这里为什么使用
		commander:   make(chan interface{}, 1),
		interval:    interval,
		container:   container,
		confirmChan: make(chan lang.PlaceholderType),
		newTicker: func(d time.Duration) timex.Ticker {
			return timex.NewTicker(d)
		},
	}
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
func (pe *PeriodicalExecutor) Wait() {
	pe.Flush()
	pe.wgBarrier.Guard(func() {
		pe.waitGroup.Wait()
	})
}

func (pe *PeriodicalExecutor) addAndCheck(task interface{}) (interface{}, bool) {
	pe.lock.Lock()
	defer func() {
		if !pe.guarded {
			pe.guarded = true
			// defer to unlock quickly
			defer pe.backgroundFlush()
		}
		pe.lock.Unlock()
	}()

	if pe.container.AddTask(task) {
		atomic.AddInt32(&pe.inflight, 1)
		return pe.container.RemoveAll(), true
	}

	return nil, false
}

func (pe *PeriodicalExecutor) backgroundFlush() {
	threading.GoSafe(func() {
		// flush before quit goroutine to avoid missing tasks
		defer pe.Flush()

		ticker := pe.newTicker(pe.interval)
		defer ticker.Stop()

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
