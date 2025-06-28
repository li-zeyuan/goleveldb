package goleveldb

import (
	"math/rand"
	"sync"
)

type SkipNodeInt struct {
	key   int64
	value interface{}
	next  []*SkipNodeInt
}

// https://segmentfault.com/a/1190000041645807
type SkipListInt struct {
	SkipNodeInt
	mutex  sync.RWMutex
	update []*SkipNodeInt // 每层的前驱节点，下表为层数
	maxl   int            // 最大层数
	skip   int            // 层间比例
	level  int            // 当前层数
	length int32          // 节点数
}

func NewSkipListInt(skip ...int) *SkipListInt {
	list := &SkipListInt{}
	list.maxl = 32
	list.skip = 4
	list.level = 0
	list.length = 0
	list.SkipNodeInt.next = make([]*SkipNodeInt, list.maxl)
	list.update = make([]*SkipNodeInt, list.maxl)
	if len(skip) == 1 && skip[0] > 1 {
		list.skip = skip[0]
	}
	return list
}

func (list *SkipListInt) Get(key int64) interface{} {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	var prev = &list.SkipNodeInt
	var next *SkipNodeInt
	for i := list.level - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && next.key < key {
			prev = next
			next = prev.next[i]
		}
	}

	if next != nil && next.key == key {
		return next.value
	} else {
		return nil
	}
}

func (list *SkipListInt) Set(key int64, val interface{}) {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	//获取每层的前驱节点=>list.update
	var prev = &list.SkipNodeInt
	var next *SkipNodeInt
	for i := list.level - 1; i >= 0; i-- { // 从顶层到底层循环
		next = prev.next[i]
		for next != nil && next.key < key {
			prev = next
			next = prev.next[i]
		}
		list.update[i] = prev
	}

	//如果key已经存在
	if next != nil && next.key == key {
		next.value = val
		return
	}

	//随机生成新结点的层数
	level := list.randomLevel()
	if level > list.level {
		level = list.level + 1
		list.level = level
		list.update[list.level-1] = &list.SkipNodeInt
	}

	//申请新的结点
	node := &SkipNodeInt{}
	node.key = key
	node.value = val
	node.next = make([]*SkipNodeInt, level)

	//调整next指向
	for i := 0; i < level; i++ {
		node.next[i] = list.update[i].next[i]
		list.update[i].next[i] = node
	}

	list.length++
}

func (list *SkipListInt) Remove(key int64) interface{} {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	//获取每层的前驱节点=>list.update
	var prev = &list.SkipNodeInt
	var next *SkipNodeInt
	for i := list.level - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && next.key < key {
			prev = next
			next = prev.next[i]
		}
		list.update[i] = prev
	}

	//结点不存在
	node := next
	if next == nil || next.key != key {
		return nil
	}

	//调整next指向
	for i, v := range node.next {
		if list.update[i].next[i] == node {
			list.update[i].next[i] = v
			if list.SkipNodeInt.next[i] == nil {
				list.level -= 1
			}
		}
		list.update[i] = nil
	}

	list.length--
	return node.value
}

func (list *SkipListInt) randomLevel() int {
	i := 1
	for ; i < list.maxl; i++ {
		if rand.Int()%list.skip != 0 {
			break
		}
	}

	return i
}
