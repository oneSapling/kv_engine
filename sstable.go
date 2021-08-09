package kvstore

import (
	"fmt"
	"io"
	"os"
)

type SsTable struct {
	// sstable中的 block的大小是 4kb
	index  	*Block
	// 记录的是索引的索引，比如index_block 的offset
	footer 	Footer
	fileNUm uint64
}

// 打开一个sstable
func Open(fileName string) (*SsTable, error) {
	var table SsTable
	var err error
	file, errOpen := os.Open(fileName)
	if errOpen != nil {
		panic(fileName+"打开出错")
		return nil, err
	}
	defer file.Close()

	stat, _ := file.Stat()
	// Read the footer block
	footerSize := int64(table.footer.Size())
	if stat.Size() < footerSize {
		fmt.Println(fileName)
		return nil, ErrTableFileTooShort
	}

	_, errSeek := file.Seek(-footerSize, io.SeekEnd)
	if errSeek != nil {
		return nil, err
	}
	err = table.footer.DecodeFrom(file)
	if err != nil {
		return nil, err
	}
	// Read the index block
	var indexBlock *Block
	p := make([]byte, table.footer.IndexHandle.Size)
	n, err := file.ReadAt(p, int64(table.footer.IndexHandle.Offset))
	if err != nil || uint32(n) != table.footer.IndexHandle.Size {
		panic(fileName+"indexblock为空")
	}else {
		indexBlock = NewBlock(p)
	}

	table.index = indexBlock
	return &table, nil
}

// 使用迭代器遍历sstable
func (table *SsTable) NewSStableIterator() *SStableIterator {
	var it SStableIterator
	it.table = table
	it.indexIter = table.index.NewBlockIterator()
	return &it
}

func (table *SsTable) InternalGetCurrent(key []byte) ([]byte, error) {
	it := table.NewSStableIterator()
	it.Seek_key_in_sstableCurrent(key)
	if it.Valid() {
		internalKey := it.InternalKey()
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

// 读取一个block
func (table *SsTable) readBlock(blockHandle BlockHandle) *Block {
	p := make([]byte, blockHandle.Size)
	file,errOpen := os.Open(TableFileName(dbPath,table.fileNUm))
	if errOpen!=nil {
		panic(errOpen.Error())
	}
	defer file.Close()
	n, err := file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}
	return NewBlock(p)
}

func (table *SsTable) GetCacheBlockHandle(blockHandle BlockHandle) (cb CacheBlockHandle) {
	// 把这个字段解码之后获取数据
	return CacheBlockHandle{blockHandle.Offset,blockHandle.Size,uint32(table.fileNUm)}
}

func (table *SsTable) ReadDataBlockCurrent(blockHandle BlockHandle) *Block {
	// 创建一个指定规格的数组
	p := make([]byte, blockHandle.Size)
	file,errOpen := os.Open(TableFileName(dbPath, table.fileNUm))
	if errOpen!=nil {
		panic(errOpen.Error())
	}
	defer file.Close()
	n, err := file.ReadAt(p, int64(blockHandle.Offset))
	if err != nil || uint32(n) != blockHandle.Size {
		return nil
	}
	return NewBlock(p)
}