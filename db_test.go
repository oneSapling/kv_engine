package kvstore

import (
	"fmt"
	"testing"
)

func Test_Db2(t *testing.T) {
	db, _ := OpenDB("D:\\leveldbTest\\test")
	for i := 0; i < 100000; i++ {
		s := fmt.Sprintf("key_%06d", i)
		var tmpkey = []byte(s)
		var tmpval = []byte("csssss000010022dddd111qqwweecsssss000010022dddd111qqwwee")
		_ = db.Put(tmpkey, tmpval)
		_, err := db.Get(tmpkey)
		if err!=nil {
			fmt.Println(err.Error())
		}
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

