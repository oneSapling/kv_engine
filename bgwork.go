package kvstore

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)
// 判断是否进行compaction操作
func (db *Db) maybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		// 如果当前已经有线程开启了就直接返回
		return
	}
	db.bgCompactionScheduled = true
	endTime := time.Now()
	cTimeFlag := false
	if endTime.Sub(db.startTime).Seconds() > 15 {
		cTimeFlag = true
		db.startTime = time.Now()
	}
	if cTimeFlag {
		go db.bgCompactionThead()
	}
}

func (db *Db) bgCompactionThead() {
	db.backgroundCompactionChan()
	db.startTime = time.Now()
	db.bgCompactionScheduled = false
}

// 这里改成异步的
func (db *Db) backgroundCompactionChan() {
	version := db.Current.Copy()

	/*num := db.ReadCurrentFile()
	if num <= 0 {
		return
	}
	version, err := LoadVersion(dbPath, num)
	if err != nil {
		panic(err.Error())
		return
	}
	version.removes = make(map[string]int)
	version.compactionTimes = 0*/

	level_0_map := make(map[*FileMetaData]int) // 求出来参加到compaction中的块，方便以后在替换版本的时候清除
	for i := 0; i < len(version.Files[0]); i++ {
		level_0_map[version.Files[0][i]] = 0
	}

	comapctionFlag := false
	loopNum := 0
	for  {
		ok,count := version.DoCompactionWorkCache()
		if !ok {
			break
		}
		if loopNum >= 2 {
			break
		}
		loopNum+=count
		comapctionFlag=true
	}

	if version.compactionTimes <= 0 {
		return
	}

	tmpFileVersion := make([][]*FileMetaData,7) // 版本文件的临时数组
	tmpFileVersion[0] = make([]*FileMetaData,0)
	tmpFileVersion[1] = make([]*FileMetaData,0)
	tmpFileVersion[2] = make([]*FileMetaData,0)
	tmpFileVersion[3] = make([]*FileMetaData,0)
	tmpFileVersion[4] = make([]*FileMetaData,0)
	tmpFileVersion[5] = make([]*FileMetaData,0)
	tmpFileVersion[6] = make([]*FileMetaData,0)
	// 给临时文件数组赋值
	for i := 0; i < len(version.Files[1]); i++ {
		tmpFileVersion[1] = append(tmpFileVersion[1],version.Files[1][i])
	}
	for i := 0; i < len(version.Files[2]); i++ {
		tmpFileVersion[2] = append(tmpFileVersion[2],version.Files[2][i])
	}
	for i := 0; i < len(version.Files[3]); i++ {
		tmpFileVersion[3] = append(tmpFileVersion[3],version.Files[3][i])
	}
	for i := 0; i < len(version.Files[4]); i++ {
		tmpFileVersion[4] = append(tmpFileVersion[4],version.Files[4][i])
	}
	for i := 0; i < len(version.Files[5]); i++ {
		tmpFileVersion[5] = append(tmpFileVersion[5],version.Files[5][i])
	}
	for i := 0; i < len(version.Files[6]); i++ {
		tmpFileVersion[6] = append(tmpFileVersion[6],version.Files[6][i])
	}
	// 获取第0层的参加compaciton的文件
	for i := 0; i < len(version.Files[0]); i++ { // 如果compaction之后的版本0不为空的情况
		_,ok := level_0_map[version.Files[0][i]] // 判断之前的map中的sstable是否还有存在的，存在的话，说明需要留存
		if ok {
			level_0_map[version.Files[0][i]] = 1 // 标记为1代表不要剔除
		}
	}
	db.Current.lock.Lock()
	// 给current赋值
	if len(tmpFileVersion[6])>0 {db.Current.Files[6] = tmpFileVersion[6]}
	if len(tmpFileVersion[5])>0 {db.Current.Files[5] = tmpFileVersion[5]}
	if len(tmpFileVersion[4])>0 {db.Current.Files[4] = tmpFileVersion[4]}
	if len(tmpFileVersion[3])>0 {db.Current.Files[3] = tmpFileVersion[3]}
	if len(tmpFileVersion[2])>0 {db.Current.Files[2] = tmpFileVersion[2]}
	if len(tmpFileVersion[1])>0 {db.Current.Files[1] = tmpFileVersion[1]}
	tmp := make([]*FileMetaData,0)
	for i := 0; i < len(db.Current.Files[0]); i++ {
		flag,ok := level_0_map[db.Current.Files[0][i]] // ok代表是原来的版本信息
		if !ok||flag==1 { //!ok为新写入的文件，flag为需要留存的文件
			tmp = append(tmp,db.Current.Files[0][i])
		}
	}
	db.Current.Files[0] = tmp
	db.Current.compactionNextFileNumber = version.compactionNextFileNumber
	descriptorNumber, _ := db.Current.compactionSave()
	db.SetCurrentFile(descriptorNumber)
	db.Current.lock.Unlock()
	for fiel,_ := range db.Current.restRemoves {
		errRemove := os.Remove(fiel)
		if errRemove != nil {
			version.restRemoves[fiel] = 0
			log.Println("removeFile:"+fiel)
		}
	}
	for fiel,_ := range version.removes{
		errRemove := os.Remove(fiel)
		if errRemove != nil {
			version.restRemoves[fiel] = 0
			log.Println("removeFile:"+fiel)
		}
	}
	db.Current.restRemoves = version.restRemoves
	if comapctionFlag&&loopNum > 0 {
		log.Println("-compaction")
	}
}

func (db *Db) dbFlush() {
	imm := db.imm
	flushFlag := false
	if imm != nil {
		db.Current.WriteLevel0Table(imm)
		flushFlag = true
	}
	db.Current.lock.Lock()
	descriptorNumber, _ := db.Current.Save()
	db.SetCurrentFile(descriptorNumber)
	db.Current.lock.Unlock()
	db.imm = nil
	db.flushScheduled = false
	if flushFlag {
		log.Println("-flush")
	}
}

func (db *Db) SetCurrentFile(descriptorNumber uint64) { // 给current文件赋值最新的版本号
	tmp := TempFileName(db.name, descriptorNumber)
	ioutil.WriteFile(tmp, []byte(fmt.Sprintf("%d", descriptorNumber)), 0600)
	os.Rename(tmp, CurrentFileName(db.name))
}

func (db *Db) ReadCurrentFile() uint64 {
	b, err := ioutil.ReadFile(CurrentFileName(db.name))
	if err != nil {
		return 0
	}
	descriptorNumber, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0
	}
	return descriptorNumber
}

func (db *Db) modMaybeScheduleCompaction() {
	if db.bgCompactionScheduled {
		// 如果当前已经有线程开启了就直接返回
		return
	}
	db.bgCompactionScheduled = true

	endTime := time.Now()
	cTimeFlag := false
	if endTime.Sub(db.startTime).Seconds() >= 1 {
		cTimeFlag = true
		db.startTime = time.Now()
	}else {
		// 这里的false为了回应上一阶段的true
		db.bgCompactionScheduled = false
	}
	if cTimeFlag {
		go db.bgCompactionThead()
	}
}
