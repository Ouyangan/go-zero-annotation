package collection

import "sync"

// A Ring can be used as fixed size ring.
type Ring struct {
	// 切片，初始化时将会指定容量
	elements []interface{}
	// 写入下标
	index int
	// 锁
	lock sync.Mutex
}

// NewRing returns a Ring object with the given size n.
func NewRing(n int) *Ring {
	if n < 1 {
		panic("n should be greater than 0")
	}

	return &Ring{
		elements: make([]interface{}, n),
	}
}

// Add adds v into r.
func (r *Ring) Add(v interface{}) {
	r.lock.Lock()
	defer r.lock.Unlock()
	// 循环覆盖写，通过取余实现
	r.elements[r.index%len(r.elements)] = v
	// 写入下标+1，为下次写入做准备
	r.index++
}

// Take takes all items from r.
func (r *Ring) Take() []interface{} {
	r.lock.Lock()
	defer r.lock.Unlock()

	var size int
	var start int
	// 如果下标大于切片容量，说明已经产生了循环覆盖行为
	if r.index > len(r.elements) {
		// size最大只能为容量大小，其实就是初始化时的n
		size = len(r.elements)
		// 从最旧的数据开始读
		start = r.index % len(r.elements)
	} else {
		// 数组还未写满，size就是index
		// 从0开始读数据
		size = r.index
	}
	//保持数据写入顺序，导出切片结果
	elements := make([]interface{}, size)
	for i := 0; i < size; i++ {
		elements[i] = r.elements[(start+i)%len(r.elements)]
	}

	return elements
}
