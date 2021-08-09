package kvstore

import (
	"testing"
)

func Test_ssTable(t *testing.T) {
	var builder BlockBuilder

	item := NewInternalKey(1, TypeValue, []byte("123"), []byte("1234"))
	builder.Add(item)
	item = NewInternalKey(2, TypeValue, []byte("124"), []byte("1245"))
	builder.Add(item)
	item = NewInternalKey(3, TypeValue, []byte("125"), []byte("0245"))
	builder.Add(item)
	p := builder.Finish()

	block := NewBlock(p)
	it := block.NewBlockIterator()

	it.Seek([]byte("1244"))
	if it.Valid() {
		if string(it.InternalKey().UserKey) != "125" {
			t.Fail()
		}

	} else {
		t.Fail()
	}
}
