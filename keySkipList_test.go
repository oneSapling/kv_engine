package kvstore

import (
	"fmt"
	"strconv"
	"testing"
)

// 这个是更新的函数
func Test_KeyAccessInsert(t *testing.T) {
	skiplist := NewKeySkipList(KeyAccessComparator)

	for i := 0; i < 10; i++ {
		var acckey AccessKeyInformation
		acckey.key = []byte(strconv.Itoa(i))
		acckey.count = 1
		skiplist.Insert(&acckey)
	}

	it := skiplist.NewSkipListIterator()
	it.SeekToFirst()
	var acckey AccessKeyInformation
	acckey.key = []byte(strconv.Itoa(9))
	a := it.Key().(*AccessKeyInformation)
	fmt.Println(a.count)
	a.count++;
	skiplist.Insert(&acckey)
	b := it.Key().(*AccessKeyInformation)
	fmt.Println(b.count)
}

//查找的函数
func Test_KeyAccessSeek(t *testing.T) {
	skiplist := NewKeySkipList(KeyAccessComparator)

	for i := 0; i < 10; i++ {
		var acckey AccessKeyInformation
		acckey.key = []byte("userkey201" + strconv.Itoa(i))
		acckey.count = 7
		skiplist.Insert(&acckey)
	}
	println("///////////////////")
	it := skiplist.NewSkipListIterator()
	var acckey1 AccessKeyInformation
	acckey1.key = []byte("userkey2011")
	result := it.SeekByKey(&acckey1)
	if result != nil {
		fmt.Println(result.key)
	}else{
		fmt.Println("没找到")
	}
}

func Test_KeyAccessQueryRange(t *testing.T) {
	skiplist := NewKeySkipList(KeyAccessComparator)

	for i := 0; i < 10; i++ {
		var acckey AccessKeyInformation
		acckey.key = []byte(strconv.Itoa(i))
		acckey.count = 1
		skiplist.Insert(&acckey)
	}

	it := skiplist.NewSkipListIterator()
	start := AccessKeyInformation{[]byte("1"),0}
	end := AccessKeyInformation{[]byte("4"),0}
	result := it.QueryRange(&start,&end)
	if result != -1 {
		fmt.Println(result)
	}else{
		fmt.Println("没找到")
	}
}
