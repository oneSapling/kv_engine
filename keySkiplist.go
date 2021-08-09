package kvstore

import (
	"math/rand"
	"sync"
)

const (
	// 最大的高度
	keyMaxHeight = 12
	keyBranching = 4
)

type keySkipList struct {
	maxHeight  int
	head       *Node
	// 比较函数
	comparator Comparator
	// 读写锁
	mu         sync.RWMutex
}

type AccessKeyInformation struct {
	key []byte
	count int64
}

func NewKeySkipList(comp Comparator) *keySkipList {
	var skiplist keySkipList
	skiplist.head = newNode(nil, keyMaxHeight)
	skiplist.maxHeight = 1
	skiplist.comparator = comp
	return &skiplist
}

func (list *keySkipList) Insert(key interface{}) {
	list.mu.Lock()
	defer list.mu.Unlock()

	_, prev := list.findGreaterOrEqual(key)
	height := list.randomHeight()
	if height > list.maxHeight {
		for i := list.maxHeight; i < height; i++ {
			prev[i] = list.head
		}
		list.maxHeight = height
	}
	x := newNode(key, height)
	for i := 0; i < height; i++ {
		x.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, x)
	}
}

/**
判断这个key是否存在
 */
func (list *keySkipList) Contains(key interface{}) bool {
	list.mu.RLock()
	defer list.mu.RUnlock()
	x, _ := list.findGreaterOrEqual(key)
	if x != nil && list.comparator(x.key, key) == 0 {
		return true
	}
	return false
}

func (list *keySkipList) NewSkipListIterator() *KeyAccess_Iterator {
	var it KeyAccess_Iterator
	it.list = list
	return &it
}

func (list *keySkipList) randomHeight() int {
	height := 1
	for height < keyMaxHeight && (rand.Intn(keyBranching) == 0) {
		height++
	}
	return height
}

func (list *keySkipList) findGreaterOrEqual(key interface{}) (*Node, [keyMaxHeight]*Node) {
	var prev [keyMaxHeight]*Node
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if list.keyIsAfterNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev
			} else {
				// Switch to next list
				level--
			}
		}
	}
	return nil, prev
}

func (list *keySkipList) RangeCount(key interface{}) (*Node, *Node) {
	var prev [keyMaxHeight]*Node
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if list.keyIsNextOneNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev[0]
			} else {
				// Switch to next list
				level--
			}
		}
	}
	return nil, prev[0]
}

func (list *keySkipList) findGreaterOneAndLessOne(key interface{}) (*Node, *Node) {
	var prev [keyMaxHeight]*Node
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if list.keyIsNextOneNode(key, next) {
			x = next
		} else {
			prev[level] = x
			if level == 0 {
				return next, prev[0]
			} else {
				// Switch to next list
				level--
			}
		}
	}
	return nil, prev[0]
}

// 范围查询的时候需要先找到比start节点的位置，或者他下一个节点
// 然后从这个其实节点去遍历第0层的链表将他们的count相加即可
func (list *keySkipList) findRange(start interface{}, end interface{}) int64 {
	var prev [keyMaxHeight]*Node
	x := list.head
	level := list.maxHeight - 1
	var need_node *Node
	for true {
		// 获取本级别中下一个node
		next := x.getNext(level)
		if list.keyIsAfterNode(start, next) {
			// 找到第一个不小于key的node
			x = next
		} else {
			// prev是第一个比key小的节点
			// next是第一个比key大的节点
			prev[level] = x
			if level == 0 {
				 need_node =  next
				 break
			} else {
				// Switch to next list
				level--
			}
		}
	}
	if need_node == nil {
		return 0;
	}
	var accessSum int64
	for need_node!=nil {
		//fmt.Println(need_node.key)
		if list.keyIsBigNext(need_node.key,end) {
			accessSum += need_node.key.(*AccessKeyInformation).count
		}else {
			break
		}
		need_node =  need_node.getNext(0)
	}
	return accessSum
}

func (list *keySkipList) findLessThan(key interface{}) *Node {
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil || list.comparator(next.key, key) >= 0 {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}
func (list *keySkipList) findlast() *Node {
	x := list.head
	level := list.maxHeight - 1
	for true {
		next := x.getNext(level)
		if next == nil {
			if level == 0 {
				return x
			} else {
				level--
			}
		} else {
			x = next
		}
	}
	return nil
}

func (list *keySkipList) keyIsAfterNode(key interface{}, n *Node) bool {
	// 判断该节点是否小于key a - b <0
	return (n != nil) && (list.comparator(n.key, key) < 0)
}

func (list *keySkipList) keyIsNextOneNode(key interface{}, n *Node) bool {
	// 判断该节点是否小于key a - b <0
	return (n != nil) && (list.comparator(n.key, key) <= 0)
}

func (list *keySkipList) keyIsBigNext(target interface{}, end interface{}) bool {
	// 判断该节点是否小于key a - b <0
	return (end != nil) && (target != nil) && (list.comparator(target, end) <= 0)
}