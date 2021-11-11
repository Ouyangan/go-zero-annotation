package syncx

import (
	"runtime"
	"sync/atomic"
)

// A SpinLock is used as a lock a fast execution.
//自旋锁
type SpinLock struct {
	lock uint32
}

//阻塞式获取锁
// Lock locks the SpinLock.
func (sl *SpinLock) Lock() {
	for !sl.TryLock() {
		//让出cpu时间片让goroutines直行任务
		//防止长时间占用
		runtime.Gosched()
	}
}

//非阻塞式获取锁
// TryLock tries to lock the SpinLock.
func (sl *SpinLock) TryLock() bool {
	return atomic.CompareAndSwapUint32(&sl.lock, 0, 1)
}

//释放锁
// Unlock unlocks the SpinLock.
func (sl *SpinLock) Unlock() {
	atomic.StoreUint32(&sl.lock, 0)
}
