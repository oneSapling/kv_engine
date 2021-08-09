package kvstore

/**
memtable的实现主要用了一个skiplist
 */
type MemTable struct {
	table       *SkipList
	memoryUsage uint64
}

func New() *MemTable {
	var memTable MemTable
	memTable.table = NewSkipList(InternalKeyComparator)
	return &memTable
}

func (memTable *MemTable) NewIterator() *MemIterator {
	return &MemIterator{listIter: memTable.table.NewSkipListIterator()}
}

// 插入
func (memTable *MemTable) Add(seq uint64, valueType ValueType, key, value []byte) {
	internalKey := NewInternalKey(seq, valueType, key, value)

	memTable.memoryUsage += uint64(16 + len(key) + len(value))
	memTable.table.Insert(internalKey)
}

// 获取
func (memTable *MemTable) Get(key []byte) ([]byte, error) {
	lookupKey := LookupKey(key)

	it := memTable.table.NewSkipListIterator()
	it.Seek(lookupKey)
	if it.Valid() {
		internalKey := it.Key().(*InternalKey)
		if UserKeyComparator(key, internalKey.UserKey) == 0 {
			// 判断valueType
			if internalKey.Type == TypeValue {
				return internalKey.UserValue, nil
			} else {
				return nil, ErrDeletion
			}
		}
	}
	return nil, ErrNotFound
}

func (memTable *MemTable) ApproximateMemoryUsage() uint64 {
	return memTable.memoryUsage
}
