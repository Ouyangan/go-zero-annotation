package syncx

import "sync"

type (
	// SharedCalls is an alias of SingleFlight.
	// Deprecated: use SingleFlight.
	SharedCalls = SingleFlight

	// SingleFlight lets the concurrent calls with the same key to share the call result.
	// For example, A called F, before it's done, B called F. Then B would not execute F,
	// and shared the result returned by F which called by A.
	// The calls with the same key are dependent, concurrent calls share the returned values.
	// A ------->calls F with key<------------------->returns val
	// B --------------------->calls F with key------>returns val
	//进程内共享调用
	//同一时间多个相同的操作只会产生一次真实调用
	//其他人等待调用完成并共享
	SingleFlight interface {
		//方法调用
		Do(key string, fn func() (interface{}, error)) (interface{}, error)
		//与Do的区别在于返回值多了一个是否执行标识
		DoEx(key string, fn func() (interface{}, error)) (interface{}, bool, error)
	}
	//函数调用结果结构体
	call struct {
		//同步标识
		wg sync.WaitGroup
		//返回值
		val interface{}
		err error
	}
	//实现类
	flightGroup struct {
		calls map[string]*call
		//go中没有并发安全的map，只能依靠lock
		lock sync.Mutex
	}
)

// NewSingleFlight returns a SingleFlight.
func NewSingleFlight() SingleFlight {
	return &flightGroup{
		calls: make(map[string]*call),
	}
}

// NewSharedCalls returns a SingleFlight.
// Deprecated: use NewSingleFlight.
func NewSharedCalls() SingleFlight {
	return NewSingleFlight()
}

func (g *flightGroup) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	//创建函数调用接收者，如果已经创建则等待其他执行者完成并返回
	c, done := g.createCall(key)
	//其他人已执行，直接拿结果
	if done {
		return c.val, c.err
	}
	//我是第一个执行人，执行调用
	g.makeCall(c, key, fn)
	return c.val, c.err
}

func (g *flightGroup) DoEx(key string, fn func() (interface{}, error)) (val interface{}, fresh bool, err error) {
	c, done := g.createCall(key)
	if done {
		return c.val, false, c.err
	}

	g.makeCall(c, key, fn)
	return c.val, true, c.err
}

//创建函数调用接收者
func (g *flightGroup) createCall(key string) (c *call, done bool) {
	//并发场景下这里需要加锁
	g.lock.Lock()
	//有其他人正在执行
	if c, ok := g.calls[key]; ok {
		//第一时间释放锁
		g.lock.Unlock()
		//等待完成其他人完成调用
		c.wg.Wait()
		return c, true
	}
	//没有人执行任务
	//就由我第一个来创建接收对象
	c = new(call)
	//同步信号+1
	//共享调用的关键就在于此
	//只能有一个人执行真正的函数调用，其他人都在等待
	c.wg.Add(1)
	g.calls[key] = c
	g.lock.Unlock()

	return c, false
}

//执行函数调用
func (g *flightGroup) makeCall(c *call, key string, fn func() (interface{}, error)) {
	//执行完成时回调此函数
	defer func() {
		//加锁
		g.lock.Lock()
		//第一次看这里的逻辑很疑惑
		//为什么可以先删除map记录再执行waitGroup.Done()呢？
		//关键在于 c, ok := g.calls[key] 这里拿到的其实是指针
		//所以不管先清除map还是waitGroup.Done()其实都是可以的
		delete(g.calls, key)
		g.lock.Unlock()
		c.wg.Done()
	}()
	//执行函数
	c.val, c.err = fn()
}
