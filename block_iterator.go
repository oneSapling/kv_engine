package kvstore

import "time"

/**
leveldb中的访问都是通过迭代器的方式来实现的
 */
type BlockIterator struct {
	block *Block
	index int
}

// Returns true iff the iterator is positioned at a valid node.
func (it *BlockIterator) Valid() bool {
	if it.block == nil {
		panic(time.Now())
	}
	return it.index >= 0 && it.index < len(it.block.items)
}

func (it *BlockIterator) InternalKey() *InternalKey {
	return &it.block.items[it.index]
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *BlockIterator) Next() {
	it.index++
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *BlockIterator) Prev() {
	it.index--
}

// Advance to the first entry with a key >= target
func (it *BlockIterator) Seek(target interface{}) {
	// 二分法查询，想要的那个block
	left := 0
	right := len(it.block.items) - 1
	for left < right {
		mid := (left + right) / 2
		if UserKeyComparator(it.block.items[mid].UserKey, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == len(it.block.items)-1 {
		if UserKeyComparator(it.block.items[left].UserKey, target) < 0 {
			// not found
			left++
		}
	}
	it.index = left
}

// Advance to the first entry with a key >= target
func (it *BlockIterator) IndexBlockSeek(target interface{}) {
	// 二分法查询，想要的那个indexblock
	left := 0
	right := len(it.block.items) - 1
	for left < right {
		mid := (left + right) / 2
		if UserKeyComparator(it.block.items[mid].UserKey, target) < 0 {
			left = mid + 1
		} else {
			right = mid
		}
	}
	if left == len(it.block.items)-1 {
		if UserKeyComparator(it.block.items[left].UserKey, target) < 0 {
			// not found
			left++
		}
	}
	it.index = left
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *BlockIterator) SeekToFirst() {
	it.index = 0
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *BlockIterator) SeekToLast() {
	if len(it.block.items) > 0 {
		it.index = len(it.block.items) - 1
	}
}
