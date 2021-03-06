// 一致性哈希实现
// author: Edgar
package consistent

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// Hash 将对应的key转换成索引
type Hash func(string) uint32

// 默认的hash函数
// 测试的发现 fnv hash 函数对于 key 相差不多的
// 映射出来的 uint32 值十分相近
func hash(name string) uint32 {
	f := fnv.New32()
	f.Write([]byte(name))
	return f.Sum32()
}

// ConsistentHasher 为一致性哈希抽象接口
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

// Option 为参数选项，用来设置内部参数
type Option func(c *consistent)


// WithReplicas 自定义副本数量
func WithReplicas(count int) Option {
	return func(c *consistent) {
		c.replicas = count
	}
}


// WithHash 自定义哈希函数
func WithHash(hash Hash) Option {
	return func(c *consistent) {
		c.hash = hash
	}
}

type consistent struct {
	// 副本数量
	replicas int
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

// Add 向哈希圆环中添加一个节点
func (c *consistent) Add(slot string) {
	c.Lock()
	defer c.Unlock()
	c.add(slot)
}

func (c *consistent) hashKey(key string, i int) uint32 {
	return c.hash(strconv.Itoa(i) + key)
}

func (c *consistent) add(node string) {
	for i := 0; i < c.replicas; i++ {
		key := c.hashKey(node, i)
		c.circle = append(c.circle, key)
		c.servers[key] = node
	}
	// 增加一个节点
	c.nodes[node] = struct{}{}
	// 重新进行排序
	sort.Sort(c.circle)
}

// Get 获取到属于的server结点
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

// Delete 删除一个节点
func (c *consistent) Delete(node string) {
	c.Lock()
	defer c.Unlock()
	// 删除节点
	delete(c.nodes, node)

	// 因为在数组中删除元素不方便，这里先记录一下需要删除的数据
	// 然后如果在这里面的数据就不再添加到新的记录中
	memo := make(map[uint32]struct{})

	// 删除hash圆环中的值
	for i := 0; i < c.replicas; i++ {
		key := c.hashKey(node, i)
		memo[key] = struct{}{}
		delete(c.servers, key)
	}

	// 创建一个新的保存
	newCircle := make(uints, 0, c.circle.Len()-c.replicas)
	for i := 0; i < c.circle.Len(); i++ {
		if _, ok := memo[c.circle[i]]; !ok {
			newCircle = append(newCircle, c.circle[i])
		}
	}
	c.circle = newCircle
}

// Members 获取到所有的节点
func (c *consistent) Members() []string {
	c.RLock()
	defer c.RUnlock()
	res := make([]string, 0, len(c.nodes))
	for k := range c.nodes {
		res = append(res, k)
	}
	return res
}

// New 创建新的一致性哈希实例
func New(options ...Option) ConsistentHasher {
	c := &consistent{
		nodes:    make(map[string]struct{}),
		servers:  make(map[uint32]string),
		circle:   make([]uint32, 0),
		replicas: 20,
		hash:     hash,
	}
	for _, option := range options {
		option(c)
	}
	return c
}
