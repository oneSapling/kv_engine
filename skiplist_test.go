package kvstore

import (
	"fmt"
	"testing"
)

func Test_Insert(t *testing.T) {
	skiplist := NewSkipList(IntComparator)
	for i := 0; i < 10; i++ {
		skiplist.Insert(i)
	}
	it := skiplist.NewSkipListIterator()
	it.SeekToFirst()
	it.Seek(1)
	if it.Valid() {
		fmt.Println("没找到")
	}else{
		fmt.Println(it.Key())
	}

}
