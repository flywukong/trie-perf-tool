# trie-perf-tool
A perf tool for mpt engine press test

## Build
```bash
make 
```

## Usage

```bash
$ ./build/perftool -h       
COMMANDS:
   press-test   Press random keys into the trie database
   verify-hash  verify hash root of trie database by comparing
   help, h      Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --engine value, -e value          Engine to use, engine can be pbss-mpt,versa-mpt or secure-trie
   --datadir value, -d value         Data directory (default: "./dataset")
   --bs value, -b value              Batch size (default: 1000)
   --threads value, -t value         Number of threads (default: 10)
   --key_range value, -r value       Key range (default: 100000000)
   --min_value_size value, -m value  Minimum value size (default: 300)
   --max_value_size value, -M value  Maximum value size (default: 300)
   --delete_ratio value, --dr value  Delete ratio (default: 0)
   --runtime value, --rt value       Duration to run the benchmark (default: 1m40s)
   --help, -h                        show help
   --version, -v                     print the version
```

the result shown below:

```bash
// press test for pbss-mpt 
$ ./build/perftool -engine pbss-mpt -b 1000 -dr 0.1 -threads 1 press-test 

start to test pbss-mpt
init trie finish, begin to press kv
[2024-07-26T13:06:32.149912+08:00] Perf In Progress tps: [get=11867.61, put=10642.14, delete=1225.47, get_not_exist=0.00], block height=65 elapsed: [rw=41.473787ms, commit=21.480795ms, cal hash=1.439519ms]
[2024-07-26T13:06:35.15027+08:00] Perf In Progress tps: [get=19333.31, put=17397.98, delete=1935.33, get_not_exist=0.00], block height=123 elapsed: [rw=32.107913ms, commit=19.507491ms, cal hash=1.347806ms]
[2024-07-26T13:06:38.072403+08:00] Perf In Progress tps: [get=17795.91, put=16029.33, delete=1766.59, get_not_exist=0.00], block height=175 elapsed: [rw=37.528568ms, commit=31.796649ms, cal hash=2.746411ms]

// verify the root hash of versa-mpt and pbss-mpt, run time is 1 minute

$ ./build/perftool -b 100  -dr 0 -runtime 1m verify-hash
begin to verify root hash, the batch size of block is 100
[2024-07-26T12:41:40.731314+08:00] verify In Progress, finish compare block 1355
[2024-07-26T12:41:43.735671+08:00] verify In Progress, finish compare block 2747
[2024-07-26T12:41:46.731497+08:00] verify In Progress, finish compare block 4105
[2024-07-26T12:41:49.73112+08:00] verify In Progress, finish compare block 5485
```

LargeTrieNum用来指定写入多少个large trie，对应aptos preloaded-large-contract-count
smallTree的数量是由StorageTrieNum-LargeTrieNum计算出来的
aptos里的operation-items是用来计算更新多少次kv，用operation-items/operation_batch计算要更新的次数
StorageInitSize对应合约数大小，aptos preloaded_large_trie_items；TrieBlocks传1
