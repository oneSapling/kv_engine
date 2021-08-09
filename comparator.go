package kvstore

import "bytes"

//    <0 , if a < b -1
//    =0 , if a == b 0
//    >0 , if a > b 1
type Comparator func(a, b interface{}) int

// 测试所用的比较器
func IntComparator(a, b interface{}) int {
	aInt := a.(int)
	bInt := b.(int)
	return aInt - bInt
}

// key access data的比较器
func KeyAccessComparator(a, b interface{}) int {
	//aKey := a.([]byte)
	//bKey := b.([]byte)
	aKey := a.(*AccessKeyInformation).key
	bKey := b.(*AccessKeyInformation).key
	return bytes.Compare(aKey, bKey)
}
