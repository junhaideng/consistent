// 一致性哈希实现
// author: Edgar
package consistent

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// 将对应的key转换成索引
type Hash func(string) uint32

// 默认的hash函数
// 测试的发现 fnv hash 函数对于 key 相差不多的
// 映射出来的 uint32 值十分相近
func hash(name string) uint32 {
	f := fnv.New32()
	f.Write([]byte(name))
	return f.Sum32()
}

type ConsistentHasher interface {
	// 添加节点
	Add(slot string)
	// 删除节点
	Delete(slot string)
	// 数据对应的节点
	Get(key string) string
}

// 用来保存圆环上的节点
type uints []uint32

// 实现 sort.Interface 接口
func (u uints) Len() int {
	return len(u)
}

func (u uints) Less(i, j int) bool {
	return u[i] < u[j]
}

func (u uints) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

// 参数选项
type Option func(c *consistent)

func WithReplices(count int) Option {
	return func(c *consistent) {
		c.replices = count
	}
}

func WithHash(hash Hash) Option {
	return func(c *consistent) {
		c.hash = hash
	}
}

type consistent struct {
	// 副本数量
	replices int
	// 所有的server 节点
	nodes map[string]struct{}
	// 节点所对应的server
	servers map[uint32]string
	// 保存所有的索引，也就是在hash圆环上的节点
	circle uints
	// 采用的hash算法
	// hash 方法可能直接决定节点的分布情况
	hash Hash
	sync.RWMutex
}

func (c *consistent) Add(slot string) {
	c.Lock()
	defer c.Unlock()
	c.add(slot)
}

func (c *consistent) hashKey(key string, i int) uint32 {
	return c.hash(strconv.Itoa(i) + key)
}

func (c *consistent) add(node string) {
	for i := 0; i < c.replices; i++ {
		key := c.hashKey(node, i)
		c.circle = append(c.circle, key)
		c.servers[key] = node
	}
	// 增加一个节点
	c.nodes[node] = struct{}{}
	// 重新进行排序
	sort.Sort(c.circle)
}

// 获取到属于的server结点
func (c *consistent) Get(name string) string {
	c.RLock()
	defer c.RUnlock()
	// 首先将hash找到
	key := c.hash(name)
	// 然后在Hash圆环上找到对应的节点
	i := sort.Search(len(c.circle), func(i int) bool { return c.circle[i] >= key })
	if i >= c.circle.Len() {
		i = 0
	}
	return c.servers[c.circle[i]]
}

// 删除一个节点
func (c *consistent) Delete(node string) {
	c.Lock()
	defer c.Unlock()
	// 删除节点
	delete(c.nodes, node)

	// 因为在数组中删除元素不方便，这里先记录一下需要删除的数据
	// 然后如果在这里面的数据就不再添加到新的记录中
	memo := make(map[uint32]struct{})

	// 删除hash圆环中的值
	for i := 0; i < c.replices; i++ {
		key := c.hashKey(node, i)
		memo[key] = struct{}{}
		delete(c.servers, key)
	}

	// 创建一个新的保存
	newCircle := make(uints, 0, c.circle.Len()-c.replices)
	for i := 0; i < c.circle.Len(); i++ {
		if _, ok := memo[c.circle[i]]; !ok {
			newCircle = append(newCircle, c.circle[i])
		}
	}
	c.circle = newCircle
}

// 获取到所有的节点
func (c *consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	res := make([]string, 0, len(c.nodes))
	for k := range c.nodes {
		res = append(res, k)
	}
	return res
}

// 创建新的实例
func New(options ...Option) ConsistentHasher {
	c := &consistent{
		nodes:    make(map[string]struct{}),
		servers:  make(map[uint32]string),
		circle:   make([]uint32, 0),
		replices: 20,
		hash:     hash,
	}
	for _, option := range options {
		option(c)
	}
	return c
}
