package kvstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type BlockBuilder struct {
	buf     bytes.Buffer
	counter uint32
}

func (blockBuilder *BlockBuilder) Reset() {
	blockBuilder.counter = 0
	blockBuilder.buf.Reset()
}

func (blockBuilder *BlockBuilder) Add(item *InternalKey) error {
	blockBuilder.counter++
	return item.EncodeTo(&blockBuilder.buf)
}

func (blockBuilder *BlockBuilder) Finish() []byte {
	// binary.Write只能持久化整数类型的字段，速所以需要对key进行编码
	err := binary.Write(&blockBuilder.buf, binary.LittleEndian, blockBuilder.counter)
	if err != nil {
		fmt.Println(err.Error())
	}
	return blockBuilder.buf.Bytes()
}

func (blockBuilder *BlockBuilder) CurrentSizeEstimate() int {
	return blockBuilder.buf.Len()
}

func (blockBuilder *BlockBuilder) Empty() bool {
	return blockBuilder.buf.Len() == 0
}
