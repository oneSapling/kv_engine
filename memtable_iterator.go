package kvstore

type MemIterator struct {
	listIter *Skip_Iterator
}

// Returns true iff the iterator is positioned at a valid node.
func (it *MemIterator) Valid() bool {
	return it.listIter.Valid()
}

func (it *MemIterator) InternalKey() *InternalKey {
	return it.listIter.Key().(*InternalKey)
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *MemIterator) Next() {
	it.listIter.Next()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *MemIterator) Prev() {
	it.listIter.Prev()
}

// Advance to the first entry with a key >= target
func (it *MemIterator) Seek(target interface{}) {
	it.listIter.Seek(target)
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *MemIterator) SeekToFirst() {
	it.listIter.SeekToFirst()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *MemIterator) SeekToLast() {
	it.listIter.SeekToLast()
}
