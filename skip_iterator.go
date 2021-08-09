package kvstore

/////////////////////////////////////////////////////////////////////////// accessKey_Iterator的方法

type KeyAccess_Iterator struct {
	list *keySkipList
	node *Node
	prev *Node
	next *Node
}

func (it *KeyAccess_Iterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.getNext(0)
}

func (it *KeyAccess_Iterator) Valid() bool {
	return it.node != nil
}

// Returns the key at the Current position.
// REQUIRES: Valid()
func (it *KeyAccess_Iterator) Key() interface{} {
	return it.node.key
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *KeyAccess_Iterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.getNext(0)
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *KeyAccess_Iterator) Prev() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findLessThan(it.node.key)
	if it.node == it.list.head {
		it.node = nil
	}
}

// Advance to the first entry with a key >= target
func (it *KeyAccess_Iterator) Seek(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
}

func (it *KeyAccess_Iterator) SeekInt(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.next, it.prev = it.list.findGreaterOneAndLessOne(target)
	//it.QueryRange(target,target)
	// println(prev[0])
}

// 查找一个指定key的函数
func (it *KeyAccess_Iterator) SeekByKey(target interface{}) *AccessKeyInformation{
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
	if it.node == nil {
		return nil
	}
	if KeyAccessComparator(target.(*AccessKeyInformation), it.node.key.(*AccessKeyInformation)) == 0 {
		return it.node.key.(*AccessKeyInformation)
	}else {
		return nil
	}
}

// Q(l,r)的函数
func (it *KeyAccess_Iterator) QueryRange(start interface{},end interface{}) int64 {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	sum := it.list.findRange(start,end)

	return sum
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *KeyAccess_Iterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findlast()
	if it.node == it.list.head {
		it.node = nil
	}
}

/////////////////////////////////////////////////////////////////////////// skipList_Iterator的方法

type Skip_Iterator struct {
	list *SkipList
	node *Node
}
// Returns true iff the iterator is positioned at a valid node.
func (it *Skip_Iterator) Valid() bool {
	return it.node != nil
}

// Returns the key at the Current position.
// REQUIRES: Valid()
func (it *Skip_Iterator) Key() interface{} {
	return it.node.key
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *Skip_Iterator) Next() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.node.getNext(0)
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *Skip_Iterator) Prev() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findLessThan(it.node.key)
	if it.node == it.list.head {
		it.node = nil
	}
}

// Advance to the first entry with a key >= target
func (it *Skip_Iterator) Seek(target interface{}) {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node, _ = it.list.findGreaterOrEqual(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Skip_Iterator) SeekToFirst() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.head.getNext(0)
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *Skip_Iterator) SeekToLast() {
	it.list.mu.RLock()
	defer it.list.mu.RUnlock()

	it.node = it.list.findlast()
	if it.node == it.list.head {
		it.node = nil
	}
}
