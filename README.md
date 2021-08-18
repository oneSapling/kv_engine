# kv store
- lsm tree based KV store
- 水友自制 kv-store
- water_db

## todo：
- 1: flush 和 compaction 为两个异步的过程（ok）
- 2: 读取快照 - 目前采用文件的方式来实现读取快照（ok）
- 3: MVCC()
- 4：WAL()
- 4: 改善flush造成的延迟尖峰 （no）
- 6: 键值分离(no)
- 7: cur
    1、区间树已经加上了
    2、