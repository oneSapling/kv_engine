package kvstore

import (
	"fmt"
	"math"
	"time"
)

type Db struct {
	name                  string
	// memtable
	mem                   *MemTable
	// immetable
	imm                   *MemTable
	Current               *Version
	nowVersionNum         int
	bgCompactionScheduled bool
	flushScheduled        bool
	startTime             time.Time
	OpenTime              time.Time
}

var dbPath string

func OpenDB(dbName string) (*Db, error) {
	dbPath = dbName
	var db Db
	db.startTime = time.Now()
	db.OpenTime  = time.Now()
	// size := 102400000
	// db.BlockCache, _ = NewBlockCacheLRU(size, nil)
	db.name = dbName
	db.mem = New()
	db.imm = nil
	db.bgCompactionScheduled = false
	db.flushScheduled = false
	num := db.ReadCurrentFile()
	if num > 0 {
		v, err := LoadVersion(dbName, num)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(v.Files[0]); i++ {
			interval,_ := NewFileInterval(string(v.Files[0][i].smallest.UserKey), string(v.Files[0][i].largest.UserKey),v.Files[0][i].Number)
			v.intervaltree.Insert(interval)
			fmt.Println("start:",string(v.Files[0][i].smallest.UserKey),", end:",string(v.Files[0][i].largest.UserKey),", fileNum:",v.Files[0][i].Number)
		}
		db.Current = v
		height := 0
		height = process(v.intervaltree.root).height
		fmt.Println("高度:",height)
		fmt.Println("建树end")
	} else {
		db.Current = NewVersion(dbName)
	}

	return &db, nil
}

type Info struct {
	 height int
}

func process(head *IntervalTreeNode) Info{
	if(head==nil){
		return Info{0}
	}
	var leftH = 0;
	var rightH = 0;
	if(head.left!=nil){
		leftH = process(head.left).height
	}
	if(head.right!=nil){
		rightH = process(head.right).height
	}
	height := int(math.Max(float64(leftH), float64(rightH)))
	return Info{height+1}
}

func (db *Db) CloseDB() {
	for db.bgCompactionScheduled {
		if !db.bgCompactionScheduled {
			break
		}
	}
	for db.flushScheduled {
		if !db.flushScheduled {
			break
		}
	}
	if db.mem.memoryUsage != 0 {
		db.imm = db.mem
		db.mem = New()
		db.dbFlush()
	}
	descriptorNumber, _ := db.Current.Save()
	db.SetCurrentFile(descriptorNumber)
}

func (db *Db) Put(key, value []byte) error {
	// 判断是否可以写
	seq, err := db.makeRoomForWrite()
	if err != nil {
		println("写入失败key为" + string(key))
		return err
	}
	// 向memtable中添加数据
	db.mem.Add(seq, TypeValue, key, value)
	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	mem := db.mem
	imm := db.imm
	value, err := mem.Get(key)
	if err == nil {
		return value, err
	}
	if imm!=nil {
		valueImm, errImm := imm.Get(key)
		if errImm == nil {
			return valueImm, nil
		}
	}
	// 将一个版本文件看作是一个存储快照
	//num := db.ReadCurrentFile()
	currentVersion := db.Current
	value, err = currentVersion.Get(key)
	return value, err
}

func (db *Db) Delete(key []byte) error {
	seq, err := db.makeRoomForWrite()
	if err != nil {
		return err
	}
	db.mem.Add(seq, TypeDeletion, key, nil)
	return nil
}

func (db *Db) makeRoomForWrite() (uint64, error) {
	// db.modMaybeScheduleCompaction() // 主要的compaction
	for true {
		if db.mem.ApproximateMemoryUsage() < Write_buffer_size {
			// 判断一下level0的大小是否可以写，如果小于设定的阈值就可以写
			if db.imm != nil {
				if !db.flushScheduled {
					// 如果当前已经有线程开启了就直接返回
					db.flushScheduled = true
					go db.dbFlush()
				}
			}
			return db.Current.NextSeq(), nil
		} else if db.imm != nil {
			// 如果当前不可变的memtable是有东西的就需要等待一下，来flush到磁盘上
			if !db.flushScheduled {
				// 如果当前已经有线程开启了就直接返回
				db.flushScheduled = true
				go db.dbFlush()
			}
		} else {
			// 如果当前的immetable是空的话，就把memtable的数据放到immetable中去
			db.imm = db.mem
			db.mem = New()
		}
	}
	return db.Current.NextSeq(), nil
}

