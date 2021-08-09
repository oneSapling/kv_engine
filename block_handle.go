package kvstore

import (
	"encoding/binary"
	"io"
)

// block的索引信息
type BlockHandle struct {
	Offset uint32
	Size   uint32
}
// block的索引信息
type CacheBlockHandle struct {
	Offset     uint32
	Size       uint32
	sstableNum uint32
}

func (blockHandle *BlockHandle) EncodeToBytes() []byte {
	p := make([]byte, 8)
	binary.LittleEndian.PutUint32(p, blockHandle.Offset)
	binary.LittleEndian.PutUint32(p[4:], blockHandle.Size)
	return p
}

func (blockHandle *BlockHandle) DecodeFromBytes(p []byte,fileNum uint64) {
	if len(p) == 8 {
		blockHandle.Offset = binary.LittleEndian.Uint32(p)
		blockHandle.Size = binary.LittleEndian.Uint32(p[4:])
		//blockHandle.sstableNum = uint32(fileNum)
	}
}

type IndexBlockHandle struct {
	Lastkey []byte
	*InternalKey
	Smallkey []byte
	sstableNUm uint64
}

func (index *IndexBlockHandle) SetBlockHandle(blockHandle BlockHandle) {
	index.UserValue = blockHandle.EncodeToBytes()
}

func (index *IndexBlockHandle) GetBlockHandle() (blockHandle BlockHandle) {
	// 把这个字段解码之后获取数据
	blockHandle.DecodeFromBytes(index.UserValue,index.sstableNUm)
	return
}

type Footer struct {
	// 首字母大写标识这个变量是公有的成员，小写的是私有的
	MetaIndexHandle BlockHandle
	IndexHandle     BlockHandle
}

func (footer *Footer) Size() int {
	// add magic Size
	return binary.Size(footer) + 8
}

func (footer *Footer) EncodeTo(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, footer)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, kTableMagicNumber)
	return err
}

//
func (footer *Footer) DecodeFrom(r io.Reader) error {
	err := binary.Read(r, binary.LittleEndian, footer)
	if err != nil {
		println(err.Error())
		return err
	}
	var magic uint64
	err = binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return err
	}
	if magic != kTableMagicNumber {
		return ErrTableFileMagic
	}
	return nil
}
