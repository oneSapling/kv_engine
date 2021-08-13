package kvstore

import (
	"os"
	"sort"
	"sync"
)

type FileMetaData struct {
	allowSeeks uint64
	Number     uint64
	fileSize   uint64
	smallest   *InternalKey
	largest    *InternalKey
}

type Version struct {
	// 控制当前版本的获取
	lock                     *sync.Mutex
	// 里面缓存sstable的元数据
	tableCache               *TableCache
	nextFileNumber           uint64
	compactionNextFileNumber uint64
	seq                      uint64
	Files                    [NumLevels][]*FileMetaData
	// 这里的层次是固定好的 7层
	compactPointer [NumLevels]*InternalKey
	// 存储compaction期间新产生的block
	blockArray  []*Block
	preVersion  *Version
	nextVersion *Version
	// 删除列表
	removes   map[string]int
	restRemoves   map[string]int
	// compactionTime
	compactionTimes      int
	// 用于检索level0的索引
	intervaltree *IntervalTree
}

func NewVersion(dbName string) *Version {
	var v Version
	v.lock = &sync.Mutex{}
	v.restRemoves = make(map[string]int)
	v.removes = make(map[string]int)
	v.tableCache = NewTableCache(dbName)
	v.nextFileNumber = 1
	v.compactionNextFileNumber = 900000
	return &v
}

func LoadVersion(dbName string, number uint64) (*Version, error) {
	fileName := DescriptorFileName(dbName, number)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	v := NewVersion(dbName)
	v.lock = &sync.Mutex{}
	v.restRemoves = make(map[string]int)
	v.removes = make(map[string]int)
	return v, v.DecodeFrom(file)
}

func (v *Version) Save() (uint64, error) {
	tmp := v.nextFileNumber
	fileName := DescriptorFileName(v.tableCache.DbName, v.nextFileNumber)
	v.nextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return tmp, err
	}
	defer file.Close()
	return tmp, v.EncodeTo(file)
}
func (v *Version) compactionSave() (uint64, error) {
	tmp := v.compactionNextFileNumber // 使用compaction自己的递增参数
	fileName := DescriptorFileName(v.tableCache.DbName, tmp)
	v.compactionNextFileNumber++
	file, err := os.Create(fileName)
	if err != nil {
		return tmp, err
	}
	defer file.Close()
	return tmp, v.EncodeTo(file)
}

func (v *Version) Copy() *Version {
	var c Version
	c.tableCache = v.tableCache
	c.nextFileNumber = v.nextFileNumber
	c.compactionNextFileNumber = v.compactionNextFileNumber
	c.seq = v.seq + 1
	for level := 0; level < NumLevels; level++ {
		c.Files[level] = make([]*FileMetaData, len(v.Files[level]))
		copy(c.Files[level], v.Files[level])
	}
	c.removes = make(map[string]int)
	c.restRemoves = make(map[string]int)
	c.compactionTimes = 0
	return &c
}

func (v *Version) NextSeq() uint64 {
	v.seq++
	return v.seq
}

// 每一层文件的个数
func (v *Version) NumLevelFiles(l int) int {
	return len(v.Files[l])
}

func (v *Version) Get(key []byte) ([]byte, error) {
	var tmp []*FileMetaData
	var tmp2 [1]*FileMetaData
	var files []*FileMetaData
	files = make([]*FileMetaData,0)
	for level := 0; level < NumLevels; level++ {
		numFiles := 0
		if len(v.Files[level]) == 0 {
			continue
		}
		if level == 0 {
			//givenInterval,_ := NewInterval(string(key),string(key))
			v.intervaltree.lock.Lock()
			//overlaps := v.intervaltree.FindOverlap(givenInterval)
			v.intervaltree.lock.Unlock()
			for i := 0; i < len(v.Files[level]); i++ {
				f := v.Files[level][i]
				if UserKeyComparator(key, f.smallest.UserKey) >= 0 && UserKeyComparator(key, f.largest.UserKey) <= 0 {
					tmp = append(tmp, f)
				}
			}
			if len(tmp) == 0 {
				continue
			}
			// 给文件按照seq号排序，这个序号越新就代表这个文件越新
			sort.Slice(tmp, func(i, j int) bool { return tmp[i].Number > tmp[j].Number })
			numFiles = len(tmp)
			files = tmp
		} else {
			// 如果不是level0的话就可以使用二分查找的方法来寻找了
			index := v.findFile(v.Files[level], key)
			if index >= len(v.Files[level]) {
				files = nil
				numFiles = 0
			} else {
				tmp2[0] = v.Files[level][index]
				if UserKeyComparator(key, tmp2[0].smallest.UserKey) < 0 {
					files = nil
					numFiles = 0
				} else {
					files = tmp2[:]
					numFiles = 1
				}
			}
		}
		// 第二步遍历这些个可疑的sstable，来找到这个key
		for i := 0; i < numFiles; i++ {
			f := files[i]
			value, err := v.tableCache.GetCurrent(f.Number, key)
			if err != ErrNotFound {
				return value, err
			}
		}
	}
	return nil, ErrNotFound
}

// 查找一个有序的级别中是否包含一个key
func (v *Version) findFile(files []*FileMetaData, key []byte) int {
	// 起点
	l := 0
	r := len(files)
	// 二分查找至少得有两个数
	for l < r {
		mid := (l + r) / 2
		f := files[mid]
		if UserKeyComparator(f.largest.UserKey, key) < 0 {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return r
}
