package kvstore

import (
	"bytes"
	"encoding/binary"
)

type Block struct {
	// 一个block包含多条记录
	items []InternalKey
}

func NewBlock(p []byte) *Block {
	var block Block
	data := bytes.NewBuffer(p)
	// 先读出block的数量
	counter := binary.LittleEndian.Uint32(p[len(p)-4:])
	// 然后解码并且把block放到items数组中
	for i := uint32(0); i < counter; i++ {
		var item InternalKey
		err := item.DecodeFrom(data)
		if err != nil {
			return nil
		}
		block.items = append(block.items, item)
	}

	return &block
}

/**
leveldb的访问全部都是用迭代器的方式
 */
func (block *Block) NewBlockIterator() *BlockIterator {
	return &BlockIterator{block: block}
}
