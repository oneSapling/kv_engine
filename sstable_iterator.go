package kvstore

type SStableIterator struct {
	table           *SsTable
	// 当前的datablock的操作指向的似乎哪一个datablock
	dataBlockHandle BlockHandle
	dataIter        *BlockIterator
	// indexblock的迭代器
	indexIter       *BlockIterator
}

// Returns true iff the iterator is positioned at a valid node.
func (it *SStableIterator) Valid() bool {
	return it.dataIter != nil && it.dataIter.Valid()
}

func (it *SStableIterator) InternalKey() *InternalKey {
	return it.dataIter.InternalKey()
}

func (it *SStableIterator) Key() []byte {
	return it.InternalKey().UserKey
}

func (it *SStableIterator) Value() []byte {
	return it.InternalKey().UserValue
}

// Advances to the next position.
// REQUIRES: Valid()
func (it *SStableIterator) Next() {
	it.dataIter.Next()
	it.skipEmptyDataBlocksForward()
}

// Advances to the previous position.
// REQUIRES: Valid()
func (it *SStableIterator) Prev() {
	it.dataIter.Prev()
	it.skipEmptyDataBlocksBackward()
}

// Advance to the first entry with a key >= target
func (it *SStableIterator) Seek_key_in_sstableCurrent(target []byte) {
	// index—block 的遍历搜寻
	it.indexIter.IndexBlockSeek(target)
	// 如果indexblock存在的话就继续

	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		var index IndexBlockHandle
		// 获取查询到的那个datablock的offerset,indexblock的uservalue就是offset
		index.Lastkey = it.indexIter.InternalKey().UserValue
		index.InternalKey = it.indexIter.InternalKey()
		index.sstableNUm = it.table.fileNUm
		tmpBlockHandle := index.GetBlockHandle()
		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {

		} else {
			// cache中没有找到，去block中读取的操作
			it.dataIter = it.table.ReadDataBlockCurrent(tmpBlockHandle).NewBlockIterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
	if it.dataIter != nil {
		// 在这个块中查找想要的那个key
		it.dataIter.Seek(target)
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SStableIterator) SeekToFirst() {
	it.indexIter.SeekToFirst()
	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToFirst()
	}
	it.skipEmptyDataBlocksForward()
}

// Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (it *SStableIterator) SeekToLast() {
	it.indexIter.SeekToLast()

	it.initDataBlock()
	if it.dataIter != nil {
		it.dataIter.SeekToLast()
	}
	it.skipEmptyDataBlocksBackward()
}

/**
BlockCache加在这个地方
 */
func (it *SStableIterator) initDataBlock() {
	if !it.indexIter.Valid() {
		it.dataIter = nil
	} else {
		// 到这里说明在index中可以找到这个范围
		//fmt.Println("查询到的index_block=",string(it.indexIter.InternalKey()))

		// todo: 通过布隆过滤器判断key是否存在（暂时不实现）

		var index IndexBlockHandle
		// 获取查询到的那个index_block
		index.Lastkey = it.indexIter.InternalKey().UserValue
		index.InternalKey = it.indexIter.InternalKey()
		tmpBlockHandle := index.GetBlockHandle()

		// cache中没有找到，去block中读取的操作
		if it.dataIter != nil && it.dataBlockHandle == tmpBlockHandle {
			// data_iter_ is already constructed with this iterator, so no need to change anything
		} else {
			it.dataIter = it.table.readBlock(tmpBlockHandle).NewBlockIterator()
			it.dataBlockHandle = tmpBlockHandle
		}
	}
}

func (it *SStableIterator) skipEmptyDataBlocksForward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Next()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToFirst()
		}
	}
}

func (it *SStableIterator) skipEmptyDataBlocksBackward() {
	for it.dataIter == nil || !it.dataIter.Valid() {
		if !it.indexIter.Valid() {
			it.dataIter = nil
			return
		}
		it.indexIter.Prev()
		it.initDataBlock()
		if it.dataIter != nil {
			it.dataIter.SeekToLast()
		}
	}
}
