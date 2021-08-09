package kvstore

type MergingIterator struct {
	list    []*SStableIterator
	current *SStableIterator
	nowkey  *InternalKey
}

func NewMergingIterator(list []*SStableIterator) *MergingIterator {
	var iter MergingIterator
	iter.list = list
	return &iter
}

// Returns true iff the iterator is positioned at a valid node.
func (it *MergingIterator) Valid() bool {
	return it.current != nil && it.current.Valid()
}

func (it *MergingIterator) InternalKey() *InternalKey {
	return it.current.InternalKey()
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *MergingIterator) Next() {
	if it.current != nil {
		it.current.Next()
	}
	it.findSmallest()
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *MergingIterator) SeekToFirst() {
	for i := 0; i < len(it.list); i++ {
		it.list[i].SeekToFirst()
	}
	it.findSmallest()
}

// 将多个排序链表再排序成一个有序的集合
// 只是在这里我们使用的是key每一个key之间是有顺序的
func (it *MergingIterator) SeekFirstKey() *InternalKey {
	for i := 0; i < len(it.list); i++ {
		// it.list[i].SeekToFirst()

	}
	return nil
}

func (it *MergingIterator) findSmallest() {
	var smallest *SStableIterator = nil
	for i := 0; i < len(it.list); i++ {
		if it.list[i].Valid() {
			if smallest == nil {
				smallest = it.list[i]
			} else if InternalKeyComparator(smallest.InternalKey(), it.list[i].InternalKey()) > 0 {
				smallest = it.list[i]
			}
		}
	}
	it.current = smallest
}
