package kvstore

import (
	"fmt"
	"io"
	"os"
)


type TableBuilder struct {
	file               *os.File
	weite              io.Writer
	// 当前的偏移，应该就是写到了哪里吧
	offset             uint32
	// 记录数
	numEntries         int32
	// block的构造
	dataBlockBuilder BlockBuilder
	// data block 的索引构造
	indexBlockBuilder  BlockBuilder
	pendingIndexEntry  bool
	pendingIndexHandle IndexBlockHandle
	// 当前的状态
	status             error
	filterBLock        int // todo:这个还没实现
	flag               int // 根据这个flag判断当前块是否属于是原来的块生成的
}

func NewTableBuilder(fileName string) *TableBuilder {
	var builder TableBuilder
	var err error
	builder.file, err = os.Create(fileName)
	if err != nil {
		return nil
	}
	builder.pendingIndexEntry = false
	return &builder
}

func (builder *TableBuilder) FileSize() uint32 {
	return builder.offset
}

func (builder *TableBuilder) Add(internalKey *InternalKey) {
	if builder.status != nil {
		return
	}
	if builder.pendingIndexEntry { // 是否要把block信息放到indexblock里面去
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// todo : filter block 布隆过滤器的部分还没有做

	builder.pendingIndexHandle.InternalKey = internalKey
	if builder.pendingIndexHandle.Smallkey == nil {
		builder.pendingIndexHandle.Smallkey = internalKey.UserValue
	}
	if builder.pendingIndexHandle.Lastkey == nil {
		builder.pendingIndexHandle.Lastkey = internalKey.UserValue
	}
	// 实体的数量
	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MaxBlockSize {
		// 当这个data block的大小大4kb的时候就把它flush到文件里面去
		builder.flush()
	}
}

func (builder *TableBuilder) Compaction_Add(internalKey *InternalKey) {
	if builder.status != nil {
		return
	}
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// todo : filter block 布隆过滤器的部分还没有做

	builder.pendingIndexHandle.InternalKey = internalKey
	// 实体的数量
	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MaxBlockSize {
		// 当这个data block的大小大4kb的时候就把它flush到文件里面去
		builder.flush()
	}
}

func (builder *TableBuilder) CompactionAddCache(internalKey *InternalKey) {
	if builder.status != nil {}
	if builder.pendingIndexEntry {
		builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		builder.pendingIndexEntry = false
	}
	// todo : filter block 布隆过滤器的部分还没有做

	builder.pendingIndexHandle.InternalKey = internalKey
	// 实体的数量
	builder.numEntries++
	builder.dataBlockBuilder.Add(internalKey)
	if builder.dataBlockBuilder.CurrentSizeEstimate() > MaxBlockSize {
		// 当这个data block的大小大4kb的时候就把它flush到文件里面去
		builder.flushReturnBlock()
	}
}

func (builder *TableBuilder) flushReturnBlock() () {
	if builder.dataBlockBuilder.Empty() {
		return
	}
	orgKey := builder.pendingIndexHandle.InternalKey
	builder.pendingIndexHandle.InternalKey = NewInternalKey(orgKey.Seq, orgKey.Type, orgKey.UserKey, nil)
	// 记录下起始点和偏移量
	newBlockHandle := builder.writeblockCompaction(&builder.dataBlockBuilder)
	// 给index_block赋值
	builder.pendingIndexHandle.SetBlockHandle(newBlockHandle)
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) writeblockCompaction(blockBuilder *BlockBuilder) (BlockHandle) {
	// 把block编码得带block的内容
	content := blockBuilder.Finish()
	// todo : compress, crc block的压缩操作还没有做
	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	// 记录一下当前的位置,加上写入的新内容。
	builder.offset += uint32(len(content))
	// 然后开始写入操作
	_, builder.status = builder.file.Write(content)
	if builder.status!=nil {
		fmt.Println(builder.status.Error())
	}
	err := builder.file.Sync()
	if err != nil {
		fmt.Println(err.Error())
	}
	blockBuilder.Reset()
	return blockHandle
}

func (builder *TableBuilder) flush() {
	/*fmt.Println("flush-start")
	fmt.Println("pend-bool:",builder.pendingIndexEntry)
	fmt.Println("flush-end")*/
	if builder.dataBlockBuilder.Empty() {
		return
	}
	// todo:给block做压缩
	orgKey := builder.pendingIndexHandle.InternalKey
	builder.pendingIndexHandle.InternalKey = NewInternalKey(orgKey.Seq, orgKey.Type, orgKey.UserKey, nil)
	// 记录下起始点和偏移量
	newBlockHandle := builder.writeblock(&builder.dataBlockBuilder)
	builder.pendingIndexHandle.SetBlockHandle(newBlockHandle)
	builder.pendingIndexEntry = true
}

func (builder *TableBuilder) lastFlush() {
	if builder.dataBlockBuilder.Empty() {
		return
	}
	// todo:给block做压缩
	orgKey := builder.pendingIndexHandle.InternalKey
	builder.pendingIndexHandle.InternalKey = NewInternalKey(orgKey.Seq, orgKey.Type, orgKey.UserKey, nil)
	// 记录下起始点和偏移量
	newBlockHandle := builder.writeblock(&builder.dataBlockBuilder)
	builder.pendingIndexHandle.SetBlockHandle(newBlockHandle)
	builder.pendingIndexEntry = true
}

/**
	在所有的datablock写完以后就需要调用这个函数，把文件尾部的信息就写完
	包括footer和
	index-block和
	filter-block和
	filter-block的索引
 */
func (builder *TableBuilder) FinishReturnBlock() {
	builder.flushReturnBlock()
	// todo : filter block
	// write index block
	// 最后将block的索引写入到文件之中去
	if builder.pendingIndexEntry {
		err1 := builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		if err1!=nil {
			fmt.Println(err1.Error())
		}
		builder.pendingIndexEntry = false
	}
	var footer Footer
	// 把 indexblock 记录下来
	footer.IndexHandle = builder.writeblock(&builder.indexBlockBuilder)
	// write footer block
	err2 := footer.EncodeTo(builder.file)
	if err2!=nil {
		fmt.Println(err2.Error())
	}
	err3 := builder.file.Close()
	if err3!= nil{
		fmt.Println(err3.Error())
	}
}

func (builder *TableBuilder) Finish() (error) {
	builder.flush()
	// write index block
	// 最后将block的索引写入到文件之中去
	if builder.pendingIndexEntry {
		err1 := builder.indexBlockBuilder.Add(builder.pendingIndexHandle.InternalKey)
		if err1!=nil {
			fmt.Println(err1.Error())
		}
		builder.pendingIndexEntry = false
	}
	var footer Footer
	// 把 indexblock 记录下来
	footer.IndexHandle = builder.writeblock(&builder.indexBlockBuilder)

	// write footer block
	err2 := footer.EncodeTo(builder.file)
	if err2!=nil {
		fmt.Println(err2.Error())
	}
	err3 := builder.file.Close()
	if err3!= nil{
		fmt.Println(err3.Error())
	}
	return nil
}

// 写入完成以后返回block的索引信息
func (builder *TableBuilder) writeblock(blockBuilder *BlockBuilder) BlockHandle {
	// 把block编码得带block的内容
	content := blockBuilder.Finish()
	// todo : compress, crc block的压缩操作还没有做
	var blockHandle BlockHandle
	blockHandle.Offset = builder.offset
	blockHandle.Size = uint32(len(content))
	// 记录一下当前的位置
	builder.offset += uint32(len(content))
	// 然后开始写入操作
	_, builder.status = builder.file.Write(content)
	if builder.status!=nil {
		fmt.Println(builder.status.Error())
	}
	err := builder.file.Sync() // 同步提交缓冲区中的文文件
	if err != nil {
		fmt.Println(err.Error())
	}
	blockBuilder.Reset()
	return blockHandle
}
