package kvstore

import (
	"fmt"
	"testing"
)

func Test_Version_Get(t *testing.T) {
	v := NewVersion("D:\\")
	var f FileMetaData
	f.Number = 123
	f.smallest = NewInternalKey(1, TypeValue, []byte("123"), nil)
	f.largest = NewInternalKey(1, TypeValue, []byte("125"), nil)
	v.Files[0] = append(v.Files[0], &f)

	//value, err := v.Get([]byte("125"))
}

func Test_Version_Load(t *testing.T) {
	v := NewVersion("D:\\leveldbtest")
	memTable := New()
	memTable.Add(1234567, TypeValue, []byte("aadsa34a"), []byte("bb23b3423"))
	v.WriteLevel0Table(memTable)
	n, _ := v.Save()
	fmt.Println(v)

	v2, _ := LoadVersion("D:\\leveldbtest", n)
	fmt.Println(v2)
	//value, err := v2.Get([]byte("aadsa34a"))
}

func Test_Version_test(t *testing.T){
	// 这个num就是在db文件中manfest中最后的那个数字
	//v2, _ := LoadVersion("D:\\leveldbTest", 2)
	//blockcache,err := lru.New(2345)
	//if err != nil{

	//}
	//value,err := v2.Get([]byte("8"),blockcache)
	//fmt.Println(string(value))
}

func Test_demo2(t *testing.T){
	println(UserKeyComparator([]byte("8"),[]byte("7979797")))
}
