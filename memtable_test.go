package kvstore

import (
	"fmt"
	"strconv"
	"testing"
)

func Test_MemTable(t *testing.T) {
	memTable := New()
	for i := 0; i < 10000; i++ {
		memTable.Add(1234567, TypeValue, []byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < 10000; i++ {
		value, err := memTable.Get([]byte(strconv.Itoa(i)))
		if err != nil {
			println(err.Error())
		}else {
			fmt.Println(string(value))
		}
	}
	fmt.Println(memTable.ApproximateMemoryUsage())
}

func Test_MemTable2(t *testing.T) {
	memTable := New()
	memTable.Add(001, TypeValue, []byte("usertable:user6284785159202261844"), []byte("1"))
	memTable.Add(001, TypeValue, []byte("usertable:user6284786258713890055"), []byte("1"))
	memTable.Add(001, TypeValue, []byte("usertable:user6284784059690633633"), []byte("1"))
	memTable.Add(001, TypeValue, []byte("usertable:user6284787358225518266"), []byte("1"))
	imm := memTable
	memTable = New()
	value, err := imm.Get([]byte("usertable:user6284784059690633638"))
	if err == ErrNotFound  {
		fmt.Println(string(value))
	}else {
		println(err.Error())

	}
	fmt.Println(memTable.ApproximateMemoryUsage())
}
func TestMem1(t *testing.T) {
	var m *MemTable
	fmt.Println(m.Get([]byte("ddddd")))
}
