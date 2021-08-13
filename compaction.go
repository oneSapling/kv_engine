package kvstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

type Compaction struct {
	// 寻找compaction的level是哪一个等级的
	level int
	// 寻找compaction的level是哪一个等级的
	inputs [2][]*FileMetaData
	// 就是这个级别的文件
	levelInput []*FileMetaData
	// 与这个级别有相加的文件
	overlapThisInput []*FileMetaData
}

func (c *Compaction) isTrivialMove() bool {
	return len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0
}

func (c *Compaction) Log() {
	//log.Printf("Compaction, level:%d", c.level)
	for i := 0; i < len(c.inputs[0]); i++ {
		//log.Printf("inputs[0]: %d", c.inputs[0][i].Number)
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		//log.Printf("inputs[1]: %d", c.inputs[1][i].Number)
	}
}

func (meta *FileMetaData) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, meta.allowSeeks)
	binary.Write(w, binary.LittleEndian, meta.fileSize)
	binary.Write(w, binary.LittleEndian, meta.Number)
	meta.smallest.EncodeTo(w)
	meta.largest.EncodeTo(w)
	return nil
}

func (meta *FileMetaData) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &meta.allowSeeks)
	binary.Read(r, binary.LittleEndian, &meta.fileSize)
	binary.Read(r, binary.LittleEndian, &meta.Number)
	meta.smallest = new(InternalKey)
	meta.smallest.DecodeFrom(r)
	meta.largest = new(InternalKey)
	meta.largest.DecodeFrom(r)
	return nil
}

func (v *Version) EncodeTo(w io.Writer) error {
	binary.Write(w, binary.LittleEndian, v.nextFileNumber)
	binary.Write(w, binary.LittleEndian, v.seq)
	binary.Write(w, binary.LittleEndian, v.compactionNextFileNumber)
	for level := 0; level < NumLevels; level++ {
		binary.Write(w, binary.LittleEndian, int32(len(v.Files[level])))
		for i := 0; i < len(v.Files[level]); i++ {
			v.Files[level][i].EncodeTo(w)
		}
	}
	return nil
}

func (v *Version) DecodeFrom(r io.Reader) error {
	binary.Read(r, binary.LittleEndian, &v.nextFileNumber)
	binary.Read(r, binary.LittleEndian, &v.seq)
	binary.Read(r, binary.LittleEndian, &v.compactionNextFileNumber)
	var numFiles int32
	for level := 0; level < NumLevels; level++ {
		binary.Read(r, binary.LittleEndian, &numFiles)
		v.Files[level] = make([]*FileMetaData, numFiles)
		for i := 0; i < int(numFiles); i++ {
			var meta FileMetaData
			meta.DecodeFrom(r)
			v.Files[level][i] = &meta
		}
	}
	return nil
}

func (v *Version) deleteFile(level int, meta *FileMetaData) {
	numFiles := len(v.Files[level])
	for i := 0; i < numFiles; i++ {
		if v.Files[level][i].Number == meta.Number {
			v.Files[level] = append(v.Files[level][:i], v.Files[level][i+1:]...)
			//log.Printf("deleteFile, level:%d, num:%d", level, meta.Number)
			break
		}
	}
}

func (v *Version) deleteFileTrue(level int, meta *FileMetaData) {
	numFiles := len(v.Files[level])
	for i := 0; i < numFiles; i++ {
		if v.Files[level][i].Number == meta.Number {
			v.Files[level] = append(v.Files[level][:i], v.Files[level][i+1:]...)
			filename := TableFileName(dbPath,meta.Number)
			v.removes[filename] = 0
			break
		}
	}
}

func (v *Version) addFile(level int, meta *FileMetaData) {
	if level == 0 {
		// 0层没有排序
		v.Files[level] = append(v.Files[level], meta)
	} else {
		numFiles := len(v.Files[level])
		index := v.findFile(v.Files[level], meta.smallest.UserKey)
		if index >= numFiles {
			v.Files[level] = append(v.Files[level], meta)
		} else {
			var tmp []*FileMetaData
			tmp = append(tmp, v.Files[level][:index]...)
			tmp = append(tmp, meta)
			v.Files[level] = append(tmp, v.Files[level][index:]...)
		}
	}
}

func (v *Version) WriteLevel0Table(imm *MemTable) {
	iter := imm.NewIterator()
	iter.SeekToFirst()
	var list []*FileMetaData
	for ; iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.Number = v.nextFileNumber
		v.nextFileNumber++
		builder := NewTableBuilder(TableFileName(v.tableCache.DbName, meta.Number))
		for ; iter.Valid(); iter.Next() {
			if meta.smallest==nil {
				meta.smallest = iter.InternalKey()
			}
			meta.largest = iter.InternalKey()
			// 新产生的block
			builder.CompactionAddCache(iter.InternalKey())
			if builder.FileSize() >= MaxFileSize {
				break
			}
		}
		builder.FinishReturnBlock()
		meta.fileSize = uint64(builder.FileSize())
		if meta.smallest != nil && meta.largest != nil {
			list = append(list, &meta)
		}
	}
	for i := 0; i < len(list); i++ {
		v.lock.Lock()

		if list[i].smallest != nil && list[i].largest != nil {
			//v.intervaltree.lock.Lock()
			interval,_ := NewFileInterval(string(list[i].smallest.UserKey), string(list[i].largest.UserKey), list[i].Number)
			v.intervaltree.Insert(interval)
			//v.intervaltree.lock.Unlock()
		}

		v.addFile(0, list[i])
		v.lock.Unlock()
	}
}

func (v *Version) WriteLevel0TableMutil(imm *MemTable) {
	List := make([]FileMetaData,0)
	var meta FileMetaData
	meta.allowSeeks = 1 << 30
	meta.Number = v.nextFileNumber
	v.nextFileNumber++
	builder := NewTableBuilder((TableFileName(v.tableCache.DbName, meta.Number)))
	iter := imm.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		for ; iter.Valid(); iter.Next() {
			if meta.smallest == nil {
				meta.smallest = iter.InternalKey()
			}
			meta.largest = iter.InternalKey()
			builder.Add(iter.InternalKey())
			if builder.FileSize() > MaxFileSize {
				_ = builder.Finish()
				meta.fileSize = uint64(builder.FileSize())
				List = append(List,meta)

				meta = FileMetaData{fileSize: 0,Number: v.nextFileNumber,allowSeeks: 1 << 30,smallest: nil,largest: nil}
				v.nextFileNumber++
				builder = NewTableBuilder((TableFileName(v.tableCache.DbName, meta.Number)))
			}
		}
		if builder.FileSize()>0 {
			_ = builder.Finish()
			meta.fileSize = uint64(builder.FileSize())
			List = append(List,meta)
		}
	}
	v.lock.Lock()
	for _,fileMeta := range List{
		v.addFile(0, &fileMeta)
	}
	v.lock.Unlock()
}

func (v *Version) overlapInLevel(level int, smallestKey, largestKey []byte) bool {
	numFiles := len(v.Files[level])
	if numFiles == 0 {
		return false
	}
	if level == 0 {
		for i := 0; i < numFiles; i++ {
			f := v.Files[level][i]
			if UserKeyComparator(smallestKey, f.largest.UserKey) > 0 || UserKeyComparator(f.smallest.UserKey, largestKey) > 0 {
				continue
			} else {
				return true
			}
		}
	} else {
		index := v.findFile(v.Files[level], smallestKey)
		if index >= numFiles {
			return false
		}
		if UserKeyComparator(largestKey, v.Files[level][index].smallest.UserKey) > 0 {
			return true
		}
	}
	return false
}

// major compaction
// 这了的block为了把comapction以后的新块放到缓存中去
func (v *Version) DoCompactionWork() bool {
	// 找到合适的级别去做compaction
	/**
	在rocksdb中的compaction有几种情况：
	1. 这个级别的size超过了的设定的值
	2. 这个级别中有需要删除的值，删除的标识叫做 tombstone
	*/
	c := v.pickCompaction()
	if c == nil {
		return false
	}
	//log.Printf("DoCompactionWork begin\n")
	//defer log.Printf("DoCompactionWork end\n")
	//c.Log()
	if c.isTrivialMove() {
		// Move file to next level
		v.deleteFile(c.level, c.inputs[0][0])
		v.addFile(c.level+1, c.inputs[0][0])
		return true
	}

	var list []*FileMetaData
	var current_key *InternalKey
	iter := v.makeInputIterator(c)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() { // 在这一次循环中sstable号是一样的
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.Number = v.nextFileNumber
		v.nextFileNumber++
		builder := NewTableBuilder((TableFileName(v.tableCache.DbName, meta.Number)))

		meta.smallest = iter.InternalKey()
		for ; iter.Valid(); iter.Next() {
			if current_key != nil {
				// 去除重复的记录
				ret := UserKeyComparator(iter.InternalKey().UserKey, current_key.UserKey)
				if ret == 0 {
					//if iter.InternalKey().Seq > current_key.Seq {
					//current_key = iter.InternalKey()
					//}
					continue
				} else if ret < 0 {
					log.Fatalf("%s < %s", string(iter.InternalKey().UserKey), string(current_key.UserKey))
				}
			}
			current_key = iter.InternalKey()
			meta.largest = iter.InternalKey()
			builder.Compaction_Add(iter.InternalKey())
			if builder.FileSize() > MaxFileSize {
				break
			}
		}

		builder.Finish()
		meta.fileSize = uint64(builder.FileSize())
		if meta.smallest != nil {
			meta.smallest.UserValue = nil
		}
		if meta.largest != nil {
			meta.largest.UserValue = nil
		}

		list = append(list, &meta)
	}

	for i := 0; i < len(c.inputs[0]); i++ {
		v.deleteFile(c.level, c.inputs[0][i])
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		v.deleteFile(c.level+1, c.inputs[1][i])
	}
	for i := 0; i < len(list); i++ {
		v.addFile(c.level+1, list[i])
	}

	return true
}

func (v *Version) DoCompactionWorkCache() (bool,int) {
	// 找到合适的级别去做compaction
	c := v.pickCompaction()
	if c == nil {
		return false,0
	}
	if c.isTrivialMove() {
		v.deleteFile(c.level, c.inputs[0][0])
		v.addFile(c.level+1, c.inputs[0][0])
		return true,0
	}
	// 开始compaction了
	//fmt.Println("进行compaction了")
	var list []*FileMetaData
	var current_key *InternalKey
	iter := v.makeInputIterator(c)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var meta FileMetaData
		meta.allowSeeks = 1 << 30
		meta.Number = v.compactionNextFileNumber
		v.compactionNextFileNumber++
		builder := NewTableBuilder(TableFileName(v.tableCache.DbName, meta.Number))
		for ; iter.Valid(); iter.Next() {
			//fmt.Println("当前获取到的key:",string(iter.InternalKey().UserKey),"sstable:",iter.current.table.fileNUm)
			if current_key != nil {
				// 去除重复的记录
				// 0 a==b
				// -1 if a < b
				// +1 if a > b
				ret := UserKeyComparator(iter.InternalKey().UserKey, current_key.UserKey)
				if ret == 0 {
					continue
				} else if ret < 0 {
					fmt.Printf("%s < %s\n", string(iter.InternalKey().UserKey), string(current_key.UserKey))
				}
			}
			current_key = iter.InternalKey()
			if meta.smallest==nil {
				meta.smallest = iter.InternalKey()
			}
			meta.largest = iter.InternalKey()
			// 新产生的block
			builder.CompactionAddCache(iter.InternalKey())
			// todo：收集完成
			if builder.FileSize() >= MaxFileSize {
				break
			}
		}
		builder.FinishReturnBlock()
		meta.fileSize = uint64(builder.FileSize())
		if meta.smallest != nil && meta.largest != nil {
			list = append(list, &meta)
		}
	}
	//fmt.Println("compaction结束了")
	for i := 0; i < len(c.inputs[0]); i++ {
		v.deleteFileTrue(c.level, c.inputs[0][i])
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		v.deleteFileTrue(c.level+1, c.inputs[1][i])
	}
	for i := 0; i < len(list); i++ {
		v.addFile(c.level+1, list[i])
	}

	return true,1
}

func (v *Version) makeInputIterator(c *Compaction) *MergingIterator {
	var list []*SStableIterator
	for i := 0; i < len(c.inputs[0]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[0][i].Number))
	}
	for i := 0; i < len(c.inputs[1]); i++ {
		list = append(list, v.tableCache.NewIterator(c.inputs[1][i].Number))
	}
	return NewMergingIterator(list)
}

// 选择都哪个级别需要合并
func (v *Version) pickCompaction() *Compaction {
	var c Compaction
	// 选出哪一个level需要合并（根据分数来判断哪一个级别需要）
	c.level = v.pickCompactionLevel()
	if c.level < 0 {
		// 如果没有选出level的话就不用做任何操作
		return nil
	}else{
		v.compactionTimes++
	}
	var smallest, largest *InternalKey
	// Files in level 0 may overlap each other, so pick up all overlapping ones
	num_compaction := 0
	if c.level == 0 {
		// 把第0层文件都取到内存中
		// c.inputs[0] = append(c.inputs[0], v.Files[c.level]...)
		for i := 0; i < len(v.Files[0]); i++ {
			num_compaction++
			c.inputs[0] = append(c.inputs[0], v.Files[0][i])
			if num_compaction >= 4{
				break;
			}
		}
		smallest = c.inputs[0][0].smallest
		largest = c.inputs[0][0].largest
		for i := 1; i < len(c.inputs[0]); i++ {
			// 挨个遍历文件找出这一级别中最小的key和最大的key
			f := c.inputs[0][i]
			if InternalKeyComparator(f.largest, largest) > 0 {
				largest = f.largest
			}
			if InternalKeyComparator(f.smallest, smallest) < 0 {
				smallest = f.smallest
			}
		}
	} else {
		// Pick the first file that comes after compact_pointer_[level]
		for i := 0; i < len(v.Files[c.level]); i++ {
			f := v.Files[c.level][i]
			if v.compactPointer[c.level] == nil || InternalKeyComparator(f.largest, v.compactPointer[c.level]) > 0 {
				c.inputs[0] = append(c.inputs[0], f)
				break
			}
		}
		if len(c.inputs[0]) == 0 {
			c.inputs[0] = append(c.inputs[0], v.Files[c.level][0])
		}
		smallest = c.inputs[0][0].smallest
		largest = c.inputs[0][0].largest
	}
	// 选择重叠的块
	for i := 0; i < len(v.Files[c.level+1]); i++ {
		f := v.Files[c.level+1][i]
		if InternalKeyComparator(f.largest, smallest) < 0 || InternalKeyComparator(f.smallest, largest) > 0 {
			// "f" is completely before specified range; skip it,  // "f" is completely after specified range; skip it
		} else {
			c.inputs[1] = append(c.inputs[1], f)
		}
	}
	return &c
}

func (v *Version) pickCompactionLevel() int {
	// We treat level-0 specially by bounding the Number of Files
	// instead of Number of bytes for two reasons:
	//
	// (1) With larger write-buffer sizes, it is nice not to do too
	// many level-0 compactions.
	//
	// (2) The Files in level-0 are merged on every read and
	// therefore we wish to avoid too many Files when the individual
	// file Size is small (perhaps because of a small write-buffer
	// setting, or very high compression ratios, or lots of
	// overwrites/deletions).
	/**
	这里通过计算每一层的分数来选择, 哪一个level才是需要合并的那一个
	*/
	compactionLevel := -1
	// 划定多少compaction分数才可以达到进行compaction的那个阈值
	bestScore := 1.0
	score := 0.0
	for level := 0; level < NumLevels-1; level++ {
		if level == 0 {
			// 如果是第0层的话就用文件数量最为触发的标准
			score = (float64(len(v.Files[0])) * 1000000.0) / float64(L0_CompactionTrigger)
		} else {
			// 如果是其他层的话就使用级别的size作为分数
			score = float64(totalFileSize(v.Files[level])) / maxBytesForLevel(level)
		}

		if score > bestScore {
			bestScore = score
			compactionLevel = level
		}

	}
	// 最后选出分数最大的那个级别
	return compactionLevel
}

func totalFileSize(files []*FileMetaData) uint64 {
	var sum uint64
	for i := 0; i < len(files); i++ {
		sum += files[i].fileSize
	}
	return sum
}
func maxBytesForLevel(level int) float64 {
	// Note: the result for level zero is not really used since we set
	// the level-0 compaction threshold based on Number of Files.

	// Result for both level-0 and level-1
	result := 10. * 1048576.0
	for level > 1 {
		result *= 10
		level--
	}
	return result
}
