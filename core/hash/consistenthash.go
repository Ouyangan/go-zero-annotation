package hash

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/tal-tech/go-zero/core/lang"
	"github.com/tal-tech/go-zero/core/mapping"
)

const (
	// TopWeight is the top weight that one entry might set.
	TopWeight = 100

	minReplicas = 100
	prime       = 16777619
)

type (
	// Func defines the hash method.
	//哈希函数
	Func func(data []byte) uint64

	// A ConsistentHash is a ring hash implementation.
	//一致性哈希
	ConsistentHash struct {
		//哈希函数
		hashFunc Func
		//虚拟节点放大因子
		//todo 放大因子有什么作用呢
		replicas int
		//key到虚拟节点的映射，会有多个虚拟节点
		keys []uint64
		//虚拟节点到物理节点的映射
		ring map[uint64][]interface{}
		//物理节点映射，方便查找
		nodes map[string]lang.PlaceholderType
		//读写锁
		lock sync.RWMutex
	}
)

// NewConsistentHash returns a ConsistentHash.
//默认构造器
func NewConsistentHash() *ConsistentHash {
	return NewCustomConsistentHash(minReplicas, Hash)
}

// NewCustomConsistentHash returns a ConsistentHash with given replicas and hash func.
//有参构造器
func NewCustomConsistentHash(replicas int, fn Func) *ConsistentHash {
	if replicas < minReplicas {
		replicas = minReplicas
	}

	if fn == nil {
		fn = Hash
	}

	return &ConsistentHash{
		hashFunc: fn,
		replicas: replicas,
		ring:     make(map[uint64][]interface{}),
		nodes:    make(map[string]lang.PlaceholderType),
	}
}

// Add adds the node with the number of h.replicas,
// the later call will overwrite the replicas of the former calls.
//扩容操作，增加物理节点
func (h *ConsistentHash) Add(node interface{}) {
	h.AddWithReplicas(node, h.replicas)
}

// AddWithReplicas adds the node with the number of replicas,
// replicas will be truncated to h.replicas if it's larger than h.replicas,
// the later call will overwrite the replicas of the former calls.
//扩容操作，增加物理节点
func (h *ConsistentHash) AddWithReplicas(node interface{}, replicas int) {
	//节点支持可重入
	//所以先执行删除操作
	h.Remove(node)
	//不能超过放大因子上限
	if replicas > h.replicas {
		replicas = h.replicas
	}
	//node key
	nodeRepr := repr(node)
	h.lock.Lock()
	defer h.lock.Unlock()
	//添加node map映射
	h.addNode(nodeRepr)
	for i := 0; i < replicas; i++ {
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		//添加虚拟节点
		h.keys = append(h.keys, hash)
		//添加虚拟节点 -> 物理节点 映射
		h.ring[hash] = append(h.ring[hash], node)
	}
	//排序，提高查询效率
	sort.Slice(h.keys, func(i, j int) bool {
		return h.keys[i] < h.keys[j]
	})
}

// AddWithWeight adds the node with weight, the weight can be 1 to 100, indicates the percent,
// the later call will overwrite the replicas of the former calls.
//按权重添加节点
//通过权重来计算方法因子，最终控制虚拟节点的数量
//权重越高，虚拟节点数量越多
func (h *ConsistentHash) AddWithWeight(node interface{}, weight int) {
	// don't need to make sure weight not larger than TopWeight,
	// because AddWithReplicas makes sure replicas cannot be larger than h.replicas
	replicas := h.replicas * weight / TopWeight
	h.AddWithReplicas(node, replicas)
}

// Get returns the corresponding node from h base on the given v.
//根据key，获取节点
func (h *ConsistentHash) Get(v interface{}) (interface{}, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	//当前哈希还上没有物理节点
	if len(h.ring) == 0 {
		return nil, false
	}
	//计算哈希值
	hash := h.hashFunc([]byte(repr(v)))
	//二分查找第一个虚拟节点
	index := sort.Search(len(h.keys), func(i int) bool {
		return h.keys[i] >= hash
	}) % len(h.keys)
	//虚拟节点->物理节点映射
	//这里可能会存在hash冲突
	//所以返回的是多个
	nodes := h.ring[h.keys[index]]
	switch len(nodes) {
	case 0:
		return nil, false
	case 1:
		return nodes[0], true
	//todo 是什么意思呢？
	default:
		innerIndex := h.hashFunc([]byte(innerRepr(v)))
		pos := int(innerIndex % uint64(len(nodes)))
		return nodes[pos], true
	}
}

// Remove removes the given node from h.
//删除物理节点
func (h *ConsistentHash) Remove(node interface{}) {
	//节点的string
	nodeRepr := repr(node)
	//并发安全
	h.lock.Lock()
	defer h.lock.Unlock()
	//如果已经移除了node节点则直接忽略
	if !h.containsNode(nodeRepr) {
		return
	}
	//移除虚拟节点映射
	for i := 0; i < h.replicas; i++ {
		//计算哈希值
		hash := h.hashFunc([]byte(nodeRepr + strconv.Itoa(i)))
		//二分查找到第一个虚拟节点
		index := sort.Search(len(h.keys), func(i int) bool {
			return h.keys[i] >= hash
		})
		//
		if index < len(h.keys) && h.keys[index] == hash {
			h.keys = append(h.keys[:index], h.keys[index+1:]...)
		}
		h.removeRingNode(hash, nodeRepr)
	}

	h.removeNode(nodeRepr)
}

func (h *ConsistentHash) removeRingNode(hash uint64, nodeRepr string) {
	if nodes, ok := h.ring[hash]; ok {
		newNodes := nodes[:0]
		for _, x := range nodes {
			if repr(x) != nodeRepr {
				newNodes = append(newNodes, x)
			}
		}
		if len(newNodes) > 0 {
			h.ring[hash] = newNodes
		} else {
			delete(h.ring, hash)
		}
	}
}

func (h *ConsistentHash) addNode(nodeRepr string) {
	h.nodes[nodeRepr] = lang.Placeholder
}

//节点是否已存在
func (h *ConsistentHash) containsNode(nodeRepr string) bool {
	//nodes本身是一个map，时间复杂度是O1效率非常高
	_, ok := h.nodes[nodeRepr]
	return ok
}

func (h *ConsistentHash) removeNode(nodeRepr string) {
	delete(h.nodes, nodeRepr)
}

//返回node的string值
//todo prime有什么作用呢？
func innerRepr(node interface{}) string {
	return fmt.Sprintf("%d:%v", prime, node)
}

//返回node的字符串
//如果让node强制实现String()会不会更好一些？
func repr(node interface{}) string {
	return mapping.Repr(node)
}
