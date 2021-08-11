package kvstore

const (
	L0_CompactionTrigger     = 4
	Write_buffer_size        = (2<<20)*4
	NumLevels                = 7
	// 2M
	MaxFileSize              = 2<<20
	// 默认的block的大小 4kb
	MaxBlockSize             = 4*1024
	kTableMagicNumber uint64 = 0xdb4775248b80fb57
)
