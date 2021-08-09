# kv store
- lsm tree based KV store
- 水友自制 kv-store
- water_db

## todo：
- 1: flush 和 compaction 为两个异步的过程（ok）
- 2: 读取快照 - 目前采用文件的方式来实现读取快照（ok）
- 3: 改善flush造成的延迟尖峰 （next）
- 4: 键值分离