package kvstore

import (
	"fmt"
	"testing"
)

func Test_Db2(t *testing.T) {
	db, _ := OpenDB("D:\\leveldbTest\\test")
	for i := 0; i < 10000000; i++ {
		s := fmt.Sprintf("key_%06d", i)
		var tmpkey = []byte(s)
		var tmpval = []byte("csssss0cssssscsssss000010022dddd1ccsssss000010022dddd111qqwweecsssss000010022dddd111qqwweesssss000010022dddd111qqwweecsssss000010022dddd111qqwwee11qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwweecsssss000010022dddd111qqwwee0csssss000010022dddd111qqwweecsssss000010022dddd111qqwwee00010022dddd111qqwweecsssss000010022dddd111qqwwee00010022dddd111qqwweecsssss000010022dddd111qqwwee")
		_ = db.Put(tmpkey, tmpval)
	}
	for i := 0; i < 100000; i++ {
		var tmpkey = []byte(fmt.Sprintf("key_%06d", i))
		_, err := db.Get(tmpkey)
		if err!=nil {
			fmt.Println(err.Error())
		}
	}
	db.CloseDB()
}
func Test_silce(t *testing.T) {
	i := make([]int,0)
	i = append(i,1)
	i = append(i,2)
	i = append(i,3)
	i = append(i,4)
	i = append(i,5)
	i = append(i,6)
	i = append(i,7)
	i = append(i,8)
	i = append(i,9)
	i = append(i,10)
	b := make([]int,0)
	// b = copy(i,i[:4])
	b[0] = 10
	fmt.Println(b[0])
}
