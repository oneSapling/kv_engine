package kvstore

import (
	"sync"

	"github.com/hashicorp/golang-lru"
)

type TableCache struct {
	mu     sync.Mutex
	DbName string
	// 这里的tableCache实现就直接使用了一个github上原有的lru
	cache  *lru.Cache
}

func NewTableCache(dbName string) *TableCache {
	var tableCache TableCache
	tableCache.DbName = dbName
	// 创建一个默认为8M的缓存
	tableCache.cache, _ = lru.New(8192)
	return &tableCache
}

func (tableCache *TableCache) NewIterator(fileNum uint64) *SStableIterator {
	table, _ := tableCache.findTable(fileNum)
	if table != nil {
		return table.NewSStableIterator()
	}
	return nil
}

// 从这个sstable中，查找这个key
func (tableCache *TableCache) GetCurrent(fileNum uint64, key []byte) ([]byte, error) {
	// 获取这个table
	table, err := tableCache.findTable(fileNum)
	if table != nil {
		return table.InternalGetCurrent(key)
	}

	return nil, err
}

func (tableCache *TableCache) Evict(fileNum uint64) {
	tableCache.cache.Remove(fileNum)
}

func (tableCache *TableCache) findTable(fileNum uint64) (*SsTable, error) {
	tableCache.mu.Lock()
	defer tableCache.mu.Unlock()
	// 如果缓存中有这个文件，就直接返回
	table, ok := tableCache.cache.Get(fileNum)
	if ok {
		return table.(*SsTable), nil
	} else {
		// 如果缓存中没有的话就需要打开这个sstable文件
		ssTable, err := Open(TableFileName(tableCache.DbName, fileNum))
		if err!=nil {
			panic(err.Error())
		}
		ssTable.fileNUm = fileNum
		tableCache.cache.Add(fileNum, ssTable)
		return ssTable, err
	}
}
