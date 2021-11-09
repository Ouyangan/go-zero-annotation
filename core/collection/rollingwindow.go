package collection

import (
	"sync"
	"time"

	"github.com/tal-tech/go-zero/core/timex"
)

type (
	// RollingWindowOption let callers customize the RollingWindow.
	RollingWindowOption func(rollingWindow *RollingWindow)

	// RollingWindow defines a rolling window to calculate the events in buckets with time interval.
	RollingWindow struct {
		//互斥锁
		lock sync.RWMutex
		//滑动窗口数量
		size int
		//窗口，数据容器
		win *window
		//滑动窗口单元时间间隔
		interval time.Duration
		//游标，用于定位当前应该写入哪个bucket
		offset int
		//汇总数据时，是否忽略当前正在写入桶的数据
		//某些场景下因为当前正在写入的桶数据并没有经过完整的窗口时间间隔
		//可能导致当前桶的统计并不准确
		ignoreCurrent bool
		//最后写入桶的时间
		//用于计算下一次写入数据间隔最后一次写入数据的之间
		//经过了多少个时间间隔
		lastTime time.Duration // start time of the last bucket
	}
)

// NewRollingWindow returns a RollingWindow that with size buckets and time interval,
// use opts to customize the RollingWindow.
func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow{
		size:     size,
		win:      newWindow(size),
		interval: interval,
		lastTime: timex.Now(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// 添加数据
func (rw *RollingWindow) Add(v float64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	//获取当前写入的下标
	rw.updateOffset()
	//添加数据
	rw.win.add(rw.offset, v)
}

//归纳汇总数据
func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	var diff int
	span := rw.span()
	//当前时间截止前，未过期桶的数量
	if span == 0 && rw.ignoreCurrent {
		diff = rw.size - 1
	} else {
		diff = rw.size - span
	}
	if diff > 0 {
		//rw.offset - rw.offset+span之间的桶数据是过期的不应该计入统计
		offset := (rw.offset + span + 1) % rw.size
		//汇总数据
		rw.win.reduce(offset, diff, fn)
	}
}

//计算当前距离最后写入数据经过多少个单元时间间隔
//实际上指的就是经过多少个桶
func (rw *RollingWindow) span() int {
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}
	//大于时间窗口时 返回窗口大小即可
	return rw.size
}

//更新当前时间的offset
//实现窗口滑动
func (rw *RollingWindow) updateOffset() {
	//经过span个桶的时间
	span := rw.span()
	//还在同一单元时间内不需要更新
	if span <= 0 {
		return
	}
	offset := rw.offset
	//既然经过了span个桶的时间没有写入数据
	//那么这些桶内的数据就不应该继续保留了，属于过期数据清空即可
	//可以看到这里全部用的 % 取余操作，可以实现按照下标周期性写入
	//如果超出下标了那就从头开始写，确保新数据一定能够正常写入
	//类似循环数组的效果
	for i := 0; i < span; i++ {
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}
	//更新offset
	rw.offset = (offset + span) % rw.size
	now := timex.Now()
	//更新操作时间
	//这里很有意思
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

//桶
type Bucket struct {
	//当前桶内值之和
	Sum float64
	//当前桶的add总次数
	Count int64
}

//向桶添加数据
func (b *Bucket) add(v float64) {
	//求和
	b.Sum += v
	//次数+1
	b.Count++
}

//桶数据清零
func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

//时间窗口
type window struct {
	//桶
	//一个桶标识一个时间间隔
	buckets []*Bucket
	//窗口大小
	size int
}

func newWindow(size int) *window {
	buckets := make([]*Bucket, size)
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

//添加数据
//offset - 游标，定位写入bucket位置
//v - 行为数据
func (w *window) add(offset int, v float64) {
	w.buckets[offset%w.size].add(v)
}

//汇总数据
//fn - 自定义的bucket统计函数
func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

//清理特定bucket
func (w *window) resetBucket(offset int) {
	w.buckets[offset%w.size].reset()
}

// IgnoreCurrentBucket lets the Reduce call ignore current bucket.
func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
