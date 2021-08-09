package kvstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"testing"
)

func Test_SsTable(t *testing.T) {
	// 这就是写入一个sstable的流程
	index := 1
	builder := NewTableBuilder("D:\\leveldbTest\\tss\\000"+strconv.Itoa(index)+".ldb")
	for i := 0; i < 100000; i++ {
		var tmpkey = []byte("10001"+ strconv.Itoa(i))
		var tmpval = []byte("GfdTnuIpkGfdTnuIpGfdTGfdTnuIpkGfdTnuIpGfdTnuIpkGfdTGfGGGfdTnuIpkGfdTnuIpGfdTnuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywzfdTnuIpkGfdTnuIpGfdTGfdTnGfdTnuIpkGfdTnuIpGfdTnuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywzuIpkGfdTnuIpGfdTnuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywznuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywzfdTnuIpkGfdTnuIpGfdTnuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywzdTnuIpkGfdTnuIpGfdTnuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywznuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywznuIpkGfdTnuIpktiFPkOywzkGfdTnuIpktiFPkOywzGfdTnuIpktiFPkOywz")
		item := NewInternalKey(1, TypeValue, tmpkey, tmpval)
		builder.Add(item)
		if builder.FileSize() > 2000000 {
			_ = builder.Finish()
			index++
			builder = NewTableBuilder("D:\\leveldbTest\\tss\\000"+strconv.Itoa(index)+".ldb")
		}
	}
	err1 := builder.Finish()
	if err1 != nil {
		fmt.Println("写入报错:"+err1.Error())
	}
	for i := 1; i < index; i++ {
		// 这是读取一个stable的流程
		table, err := Open("D:\\leveldbTest\\tss\\000"+strconv.Itoa(index)+".ldb")
		if err!=nil {
			fmt.Println(err)
		}
		it := table.NewSStableIterator()
		it.SeekToFirst()
	}
}

func Test_sstableBlock(t *testing.T) {
	table, err := Open("D:\\leveldbTest\\test\\900672.ldb")
	if err == nil {
		fmt.Println("table-index=",table.index)
		fmt.Println("table-footer=",table.footer)
	}else{
		fmt.Println(err)
	}

	it := table.NewSStableIterator()
	it.SeekToFirst()
}

type Website struct {
	Url int32
}

func TestFileWrite(t *testing.T) {
	file, err := os.Create("output.txt")
	defer file.Close()
	for i := 1; i <= 10; i++ {
		info := Website{
			int32(i),
		}
		if err != nil {
			fmt.Println("文件创建失败 ", err.Error())
			return
		}
		var bin_buf bytes.Buffer
		binary.Write(&bin_buf, binary.LittleEndian, info)
		b := bin_buf.Bytes()
		_, err = file.Write(b)
		/*if err != nil {
			fmt.Println("编码失败", err.Error())
			return
		}*/
	}
}
