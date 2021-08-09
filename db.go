package kvstore

import (
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
		db.Current = v
	} else {
		db.Current = NewVersion(dbName)
	}

	return &db, nil
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
	num := db.ReadCurrentFile()
	if num > 0 {
		currentVersion, err := LoadVersion(dbPath, num)
		if err != nil {
			return nil, err
		}
		value, err = currentVersion.Get(key)
		return value, err
	}else{
		return nil,ErrNotFound
	}
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
	db.modMaybeScheduleCompaction() // 主要的compaction
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

