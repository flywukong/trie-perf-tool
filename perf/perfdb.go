package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type DBRunner struct {
	db                    StateDatabase
	perfConfig            PerfConfig
	stat                  *Stat
	lastStatInstant       time.Time
	taskChan              chan DBTask
	initTaskChan          chan DBTask
	accountKeyCache       *InsertedKeySet
	smallStorageTrieCache *InsertedKeySet
	storageCache          map[string][]string
	largeStorageCache     map[string][]string
	blockHeight           uint64
	rwDuration            time.Duration
	rDuration             time.Duration
	wDuration             time.Duration
	commitDuration        time.Duration
	hashDuration          time.Duration
	totalRwDurations      time.Duration // Accumulated rwDuration
	totalBlockDurations   time.Duration
	totalReadCost         time.Duration
	totalWriteCost        time.Duration
	BlockCount            int64 // Number of rwDuration samples
	totalHashurations     time.Duration
	updateAccount         int64
	largeStorageTrie      []string
	smallStorageTrie      []string
	storageOwnerList      []string
	owners                []common.Hash
}

func NewDBRunner(
	db StateDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *DBRunner {
	totalTrieNum := int(config.StorageTrieNum)
	largeTrieNum := int(config.LargeTrieNum)
	runner := &DBRunner{
		db:                    db,
		stat:                  NewStat(),
		lastStatInstant:       time.Now(),
		perfConfig:            config,
		taskChan:              make(chan DBTask, taskBufferSize),
		initTaskChan:          make(chan DBTask, taskBufferSize),
		accountKeyCache:       NewFixedSizeSet(AccountKeyCacheSize),
		smallStorageTrieCache: NewFixedSizeSet(totalTrieNum - largeTrieNum),
		storageCache:          make(map[string][]string),
		largeStorageCache:     make(map[string][]string),
		largeStorageTrie:      make([]string, largeTrieNum),
		smallStorageTrie:      make([]string, totalTrieNum-largeTrieNum),
		storageOwnerList:      make([]string, totalTrieNum+MaxLargeStorageTrieNum),
		owners:                make([]common.Hash, totalTrieNum+MaxLargeStorageTrieNum),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)

	// init the state db
	//	blocks := d.perfConfig.AccountsBlocks
	totalTrieNum := int(d.perfConfig.StorageTrieNum)
	fmt.Println("init storage trie number:", totalTrieNum)

	largeTrieNum := int(d.perfConfig.LargeTrieNum)

	if d.perfConfig.IsInitMode {
		debug.SetGCPercent(80)
		debug.SetMemoryLimit(48 * 1024 * 1024 * 1024)

		/*
			fmt.Printf("init account in %d blocks , account num %d \n", blocks, d.perfConfig.AccountsInitSize)
			diskVersion := d.db.GetVersion()
			fmt.Println("disk version is", diskVersion)
			accSize := d.perfConfig.AccountsInitSize
			accBatch := d.perfConfig.AccountsBlocks
			accPerBatch := accSize / accBatch

			for i := uint64(0); i < d.perfConfig.AccountsBlocks; i++ {
				startIndex := i * accPerBatch
				d.InitAccount(i, startIndex, accPerBatch)
				if i > 1 && i%20000 == 0 {
					fmt.Println("running empty block for 20000 blocks")
					for j := uint64(0); j < d.perfConfig.AccountsBlocks/50; j++ {
						d.RunEmptyBlock(j)
					}
				}
			}


		*/
		// generate the storage owners, the first two owner is the large storage and
		// the others are small trie
		ownerList := genOwnerHashKey(totalTrieNum + MaxLargeStorageTrieNum)
		for i := 0; i < totalTrieNum+MaxLargeStorageTrieNum; i++ {
			d.storageOwnerList[i] = ownerList[i]
		}
		/*
			for j := uint64(0); j < d.perfConfig.AccountsBlocks/50; j++ {
				d.RunEmptyBlock(j)
			}


		*/
		d.InitLargeStorageTries()
		fmt.Println("init the large tries finish")

		/*
			d.InitSmallStorageTrie()
			fmt.Println("init small trie finish")

			for i := uint64(0); i < d.perfConfig.AccountsBlocks/50; i++ {
				d.RunEmptyBlock(i)
			}

		*/

		// init the lock of each tree
		d.db.InitStorage(d.owners, totalTrieNum+MaxLargeStorageTrieNum)
	} else {
		fmt.Println("reload the db and restart press test")
		ownerList := genOwnerHashKey(totalTrieNum + MaxLargeStorageTrieNum)
		for i := 0; i < totalTrieNum+MaxLargeStorageTrieNum; i++ {
			d.storageOwnerList[i] = ownerList[i]
			d.owners[i] = common.BytesToHash([]byte(d.storageOwnerList[i]))
		}

		d.updateCache(uint64(largeTrieNum), uint64(totalTrieNum))
		d.db.InitStorage(d.owners, totalTrieNum+MaxLargeStorageTrieNum)
		// repair the snapshot of state db
		d.db.RepairSnap(d.storageOwnerList, int(d.perfConfig.LargeTrieNum))
	}

	fmt.Println("init db finish, begin to press kv")
	// Start task generation thread
	go d.generateRunTasks(ctx, d.perfConfig.BatchSize)
	time.Sleep(20 * time.Second)
	d.runInternal(ctx)
}

func (d *DBRunner) updateCache(largeTrieNum, totalTrieNum uint64) {
	startUpdate := time.Now()
	defer func() {
		fmt.Println("update cache cost time", time.Since(startUpdate).Milliseconds(), "ms")
	}()
	for i := uint64(0); i < largeTrieNum; i++ {
		owner := d.storageOwnerList[i]
		d.largeStorageTrie[i] = owner
		largeStorageInitSize := d.perfConfig.StorageTrieSize

		index := mathrand.Intn(5) // 0 到 4
		// 计算取值范围的起始和结束位置
		startRange := (int(d.perfConfig.StorageTrieSize) / 5 * index)     // 当前区间的开始
		endRange := (int(d.perfConfig.StorageTrieSize) / 5 * (index + 1)) // 下一区间的开始

		// 取中间随机数
		middleRangeStart := startRange + (endRange-startRange)/4 // 区间中间起点
		middleRangeEnd := endRange - (endRange-startRange)/4     // 区间中间终点

		// 在区间中间随机选择一个位置
		randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)

		if i <= 1 {
			d.largeStorageCache[owner] = genStorageTrieKeyV1(uint64(randomIndex), largeStorageInitSize/500)
		} else {
			d.largeStorageCache[owner] = genStorageTrieKey(owner, uint64(randomIndex), largeStorageInitSize/500)
		}

		//	d.largeStorageCache[owner] = genStorageTrieKey(owner, uint64(randomIndex), largeStorageInitSize/500)
		//	fmt.Println("load large tree owner hash", common.BytesToHash([]byte(owner)))
	}

	for i := uint64(0); i < totalTrieNum-largeTrieNum; i++ {
		owner := d.storageOwnerList[i+MaxLargeStorageTrieNum]
		d.smallStorageTrieCache.Add(owner)
		d.smallStorageTrie[i] = string(owner)
		smallStorageInitSize := d.perfConfig.SmallStorageSize

		index := mathrand.Intn(5)                                         // 0 到 4
		startRange := (int(d.perfConfig.StorageTrieSize) / 5 * index)     // 当前区间的开始
		endRange := (int(d.perfConfig.StorageTrieSize) / 5 * (index + 1)) // 下一区间的开始

		// 取中间随机数
		middleRangeStart := startRange + (endRange-startRange)/4 // 区间中间起点
		middleRangeEnd := endRange - (endRange-startRange)/4     // 区间中间终点

		// 在区间中间随机选择一个位置
		randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)

		d.storageCache[owner] = genStorageTrieKey(owner, uint64(randomIndex), smallStorageInitSize/500)
		//	fmt.Println("load small tree owner hash", common.BytesToHash([]byte(owner)))
	}

	// init the account key cache
	accKeys := genAccountKey(d.perfConfig.AccountsInitSize, AccountKeyCacheSize/3)
	for i := 0; i < AccountKeyCacheSize/3; i++ {
		d.accountKeyCache.Add(accKeys[i])
	}
}

func (d *DBRunner) generateRunTasks(ctx context.Context, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			startUpdate := time.Now()
			defer func() {
				fmt.Println("generate task cost time:", time.Since(startUpdate).Milliseconds(), "ms")
			}()
			// update the source test data cache every 10000 blocks
			if d.blockHeight%5000 == 0 && d.blockHeight > 0 {
				d.updateCache(d.perfConfig.LargeTrieNum, d.perfConfig.StorageTrieNum)
			}

			taskMap := NewDBTask()
			var wg sync.WaitGroup

			wg.Add(1)
			go func(task *DBTask) {
				defer wg.Done()
				random := mathrand.New(mathrand.NewSource(0))
				updateAccounts := int(batchSize) / 5
				accounts := make([][]byte, updateAccounts)
				for i := 0; i < updateAccounts; i++ {
					var (
						nonce = uint64(random.Int63())
						root  = types.EmptyRootHash
						code  = crypto.Keccak256(generateRandomBytes(20))
					)
					numBytes := random.Uint32() % 33 // [0, 32] bytes
					balanceBytes := make([]byte, numBytes)
					random.Read(balanceBytes)
					balance := new(uint256.Int).SetBytes(balanceBytes)
					data, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code})
					accounts[i] = data
				}

				for i := 0; i < updateAccounts; i++ {
					randomKey, found := d.accountKeyCache.RandomItem()
					if found {
						// update the account
						taskMap.AccountTask[randomKey] = accounts[i]
					}
				}
			}(&taskMap)

			min_value_size := d.perfConfig.MinValueSize
			max_value_size := d.perfConfig.MaxValueSize

			// small storage trie write 3/5 kv of storage
			var randomStorageTrieList []string
			// random choose 29 small tries
			if len(d.smallStorageTrie) > SmallTriesReadInBlock {
				randomStorageTrieList = make([]string, SmallTriesReadInBlock)
				perm := mathrand.Perm(len(d.smallStorageTrie))
				for i := 0; i < SmallTriesReadInBlock; i++ {
					randomStorageTrieList[i] = d.smallStorageTrie[perm[i]]
				}
			} else {
				randomStorageTrieList = d.smallStorageTrie
			}

			storageUpdateNum := int(batchSize) / 5 * 3 / len(randomStorageTrieList)
			for i := 0; i < len(randomStorageTrieList); i++ {
				keys := make([]string, 0, storageUpdateNum)
				vals := make([]string, 0, storageUpdateNum)
				//owner := d.smallStorageTrie[i]
				owner := d.storageOwnerList[i+MaxLargeStorageTrieNum]
				v := d.storageCache[owner]
				for j := 0; j < storageUpdateNum; j++ {
					// only cache 10000 for updating test
					randomIndex := mathrand.Intn(len(v))
					keys = append(keys, v[randomIndex])
					vals = append(vals, string(generateValueV2(min_value_size, max_value_size)))
				}
				taskMap.SmallTrieTask[owner] = CAKeyValue{Keys: keys, Vals: vals}
			}

			// large storage trie write 1/5 kv of storage
			largeStorageUpdateNum := int(batchSize) / 5
			largeTrieNum := int(d.perfConfig.LargeTrieNum)
			if len(d.largeStorageTrie) != largeTrieNum {
				panic("large tree is not right")
			}
			// random choose one large tree to read and write
			index := mathrand.Intn(largeTrieNum)
			//	k := d.largeStorageTrie[index]
			owner := d.storageOwnerList[index]
			v := d.largeStorageCache[owner]
			//fmt.Println("large tree cache key len ", len(v))
			keys := make([]string, 0, largeStorageUpdateNum)
			vals := make([]string, 0, largeStorageUpdateNum)
			for j := 0; j < largeStorageUpdateNum; j++ {
				// only cache 10000 for updating test
				randomIndex := mathrand.Intn(len(v))
				value := generateValueV2(min_value_size, max_value_size)
				keys = append(keys, v[randomIndex])
				vals = append(vals, string(value))
			}
			taskMap.LargeTrieTask[owner] = CAKeyValue{Keys: keys, Vals: vals}
			d.taskChan <- taskMap
		}
	}
}

func (d *DBRunner) InitLargeStorageTrie(largeTrieIndex int) {
	StorageInitSize := d.perfConfig.StorageTrieSize
	start := time.Now()
	//	ownerHash := string(crypto.Keccak256(CAAccount[0][:]))
	ownerHash := d.storageOwnerList[largeTrieIndex]
	d.largeStorageTrie[largeTrieIndex] = ownerHash
	d.owners[largeTrieIndex] = common.BytesToHash([]byte(ownerHash))
	fmt.Println("large trie owner hash", common.BytesToHash([]byte(ownerHash)))
	blocks := d.perfConfig.TrieBlocks

	fmt.Printf("init large tree in %d blocks , trie size %d \n", blocks, StorageInitSize)
	storageBatch := StorageInitSize / blocks
	for i := uint64(0); i < d.perfConfig.TrieBlocks; i++ {
		if i%100 == 0 {
			fmt.Printf("finish init the  %d block of large trie \n", i)
		}
		vals := make([]string, 0, storageBatch)
		for j := uint64(0); j < storageBatch; j++ {
			value := generateValueV2(d.perfConfig.MinValueSize, d.perfConfig.MaxValueSize)
			//	keys = append(keys, string(randomStr))
			vals = append(vals, string(value))
		}
		keys := genStorageTrieKey(ownerHash, i*storageBatch, storageBatch)

		d.InitSingleStorageTrie(ownerHash, CAKeyValue{
			Keys: keys, Vals: vals}, false)
	}

	fmt.Println("init large storage trie success", "cost time", time.Since(start).Seconds(), "s")
	return
}

func (d *DBRunner) InitLargeStorageTries() {
	largeTrieNum := int(d.perfConfig.LargeTrieNum)
	for i := 0; i < largeTrieNum; i++ {
		d.InitLargeStorageTrie(i)
		fmt.Printf("init the  %d large trie success \n", i+1)
		for j := uint64(0); j < d.perfConfig.AccountsBlocks/50; j++ {
			d.RunEmptyBlock(j)
		}
	}
}

func (d *DBRunner) InitSmallStorageTrie() []common.Hash {
	CATrieNum := d.perfConfig.StorageTrieNum
	smallTrees := make([]common.Hash, CATrieNum-d.perfConfig.LargeTrieNum)
	initTrieNum := 0
	var StorageInitSize uint64
	// init small tree by the config trie size
	if d.perfConfig.SmallStorageSize > 0 {
		StorageInitSize = d.perfConfig.SmallStorageSize
	} else {
		if d.perfConfig.StorageTrieSize > 10000000 {
			StorageInitSize = d.perfConfig.StorageTrieSize / 50
		} else {
			StorageInitSize = d.perfConfig.StorageTrieSize / 5
		}
	}

	for i := 0; i < int(CATrieNum-d.perfConfig.LargeTrieNum); i++ {
		ownerHash := d.storageOwnerList[i+MaxLargeStorageTrieNum]
		fmt.Println("generate small trie, owner:", common.BytesToHash([]byte(ownerHash)))
		d.smallStorageTrie[i] = ownerHash
		d.owners[i+MaxLargeStorageTrieNum] = common.BytesToHash([]byte(ownerHash))
		smallTrees[i] = common.BytesToHash([]byte(ownerHash))
		blocks := d.perfConfig.TrieBlocks / 100

		storageBatch := StorageInitSize / blocks
		for t := uint64(0); t < blocks; t++ {
			vals := make([]string, 0, storageBatch)
			for j := uint64(0); j < storageBatch; j++ {
				value := generateValueV2(d.perfConfig.MinValueSize, d.perfConfig.MaxValueSize)
				vals = append(vals, string(value))
			}
			keys := genStorageTrieKey(ownerHash, t*storageBatch, storageBatch)
			if t == 0 {
				d.InitSingleStorageTrie(ownerHash, CAKeyValue{
					Keys: keys, Vals: vals}, true)
			} else {
				d.InitSingleStorageTrie(ownerHash, CAKeyValue{
					Keys: keys, Vals: vals}, false)
			}
		}
		d.smallStorageTrieCache.Add(ownerHash)
		initTrieNum++
		fmt.Printf("init small tree in %d blocks, trie szie %d, hash %v, finish trie init num %d \n",
			blocks, StorageInitSize, common.BytesToHash([]byte(ownerHash)), initTrieNum)

	}
	return smallTrees
}

func (d *DBRunner) runInternal(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	printAvg := 0

	for {
		select {
		case taskInfo := <-d.taskChan:
			rwStart := time.Now()
			startBlock := time.Now()
			// read, put or delete keys
			d.UpdateDB(taskInfo)
			d.rwDuration = time.Since(rwStart)
			d.totalRwDurations += d.rwDuration
			if d.db.GetMPTEngine() == VERSADBEngine {
				VeraDBRWLatency.Update(d.rwDuration)
			} else {
				stateDBRWLatency.Update(d.rwDuration)
			}
			// compute hash
			/*
					hashStart := time.Now()
					d.db.Hash()
					d.hashDuration = time.Since(hashStart)
					if d.db.GetMPTEngine() == VERSADBEngine {
						VeraDBHashLatency.Update(d.hashDuration)
					} else {
						stateDBHashLatency.Update(d.hashDuration)
					}
				d.totalHashurations += d.hashDuration
			*/
			// commit

			commtStart := time.Now()
			if _, err := d.db.Commit(); err != nil {
				panic("failed to commit: " + err.Error())
			}

			d.commitDuration = time.Since(commtStart)
			d.totalHashurations += d.commitDuration
			if d.db.GetMPTEngine() == VERSADBEngine {
				VeraDBCommitLatency.Update(d.commitDuration)
			} else {
				stateDBCommitLatency.Update(d.commitDuration)
			}
			if d.commitDuration.Milliseconds() > 600 {
				fmt.Println("commit cost", d.commitDuration.Milliseconds(), "ms", ", block height:", d.blockHeight)
			}

			// sleep 500ms for each block
			d.trySleep()
			d.blockHeight++

			if d.db.GetMPTEngine() == VERSADBEngine {
				VeraDBImportLatency.Update(time.Since(startBlock))
			} else {
				stateDBImportLatency.Update(time.Since(startBlock))
			}
			d.totalBlockDurations += time.Since(startBlock)

			if d.blockHeight%100 == 0 {
				fmt.Println("import block latency:", time.Since(startBlock).Milliseconds(), "ms",
					"rw time ", d.rwDuration.Milliseconds(), "ms",
					"read time", d.rDuration.Milliseconds(), "ms",
					"write time", d.wDuration.Milliseconds(), "ms",
					"commit time", d.commitDuration.Milliseconds(), "ms",
					"total cost", d.totalBlockDurations.Seconds(), "s",
					"block height", d.blockHeight)
			}
			d.updateAccount = 0
			BlockHeight.Update(int64(d.blockHeight))
		case <-ticker.C:
			d.printStat()
			printAvg++
			if printAvg%100 == 0 {
				d.printAVGStat(startTime)
			}

		case <-ctx.Done():
			fmt.Println("Shutting down")
			d.printAVGStat(startTime)
			return
		}
	}
}

func (r *DBRunner) printAVGStat(startTime time.Time) {
	fmt.Printf(
		" Avg Perf metrics: %s, block height=%d elapsed: [read =%v ms, write=%v ms, commit =%v ms]\n",
		r.stat.CalcAverageIOStat(time.Since(startTime)),
		r.blockHeight,
		float64(r.totalReadCost.Milliseconds())/float64(r.blockHeight),
		float64(r.totalWriteCost.Milliseconds())/float64(r.blockHeight),
		float64(r.totalHashurations.Milliseconds())/float64(r.blockHeight),
	)
}

func (r *DBRunner) printStat() {
	delta := time.Since(r.lastStatInstant)
	fmt.Printf(
		"[%s] Perf In Progress %s, block height=%d elapsed: [batch read=%v, batch write=%v, commit=%v, cal hash=%v]\n",
		time.Now().Format(time.RFC3339Nano),
		r.stat.CalcTpsAndOutput(delta),
		r.blockHeight,
		r.rDuration,
		r.wDuration,
		r.commitDuration,
		r.hashDuration,
	)
	r.lastStatInstant = time.Now()
}

func (r *DBRunner) InitAccount(blockNum, startIndex, size uint64) {
	addresses, accounts := makeAccountsV2(startIndex, size)

	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		startPut := time.Now()
		err := r.db.AddAccount(initKey, accounts[i])
		if err != nil {
			fmt.Println("init account err", err)
		}
		if r.db.GetMPTEngine() == VERSADBEngine {
			VersaDBAccPutLatency.Update(time.Since(startPut))
		} else {
			StateDBAccPutLatency.Update(time.Since(startPut))
		}
		r.accountKeyCache.Add(initKey)
		if r.db.GetMPTEngine() == StateTrieEngine && r.db.GetFlattenDB() != nil {
			// simulate insert key to snap
			snapDB := r.db.GetFlattenDB()
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
	}

	commtStart := time.Now()
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	r.commitDuration = time.Since(commtStart)
	if r.db.GetMPTEngine() == VERSADBEngine {
		VeraDBCommitLatency.Update(r.commitDuration)
	} else {
		stateDBCommitLatency.Update(r.commitDuration)
	}

	r.trySleep()
	fmt.Println("init db account commit success, block number", blockNum)
}

func (r *DBRunner) RunEmptyBlock(index uint64) {
	time.Sleep(500 * time.Millisecond)
	fmt.Println("run empty block, number", index)
}

func (d *DBRunner) UpdateDB(
	taskInfo DBTask,
) {

	batchSize := int(d.perfConfig.BatchSize)
	threadNum := d.perfConfig.NumJobs
	var wg sync.WaitGroup
	start := time.Now()

	smallTrieMaps := splitTrieTask(taskInfo.SmallTrieTask, threadNum-1)

	for i := 0; i < threadNum-1; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for owner, CAKeys := range smallTrieMaps[index] {
				for j := 0; j < len(CAKeys.Keys); j++ {
					startRead := time.Now()
					value, err := d.db.GetStorage([]byte(owner), []byte(CAKeys.Keys[j]))
					if d.db.GetMPTEngine() == VERSADBEngine {
						VersaDBAccGetLatency.Update(time.Since(startRead))
					} else {
						StateDBAccGetLatency.Update(time.Since(startRead))
					}
					d.stat.IncGet(1)
					if err != nil || value == nil {
						if err != nil {
							fmt.Println("fail to get small trie key", err.Error())
						}
						d.stat.IncGetNotExist(1)
					}
				}
			}
		}(i)
	}

	// use one thread to read a random large storage trie
	wg.Add(1)
	go func() {
		defer wg.Done()
		for owner, CAkeys := range taskInfo.LargeTrieTask {
			for i := 0; i < len(CAkeys.Keys); i++ {
				startRead := time.Now()
				value, err := d.db.GetStorage([]byte(owner), []byte(CAkeys.Keys[i]))
				if d.db.GetMPTEngine() == VERSADBEngine {
					versaDBStorageGetLatency.Update(time.Since(startRead))
				} else {
					StateDBStorageGetLatency.Update(time.Since(startRead))
				}
				d.stat.IncGet(1)
				if err != nil || value == nil {
					if err != nil {
						fmt.Println("fail to get large tree key", err.Error())
					}
					d.stat.IncGetNotExist(1)
				}
			}
		}
	}()

	wg.Wait()

	accountMaps := splitAccountTask(taskInfo.AccountTask, threadNum)
	//  read 1/5 kv of account
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for key, _ := range accountMaps[index] {
				startRead := time.Now()
				value, err := d.db.GetAccount(key)
				if d.db.GetMPTEngine() == VERSADBEngine {
					VersaDBAccGetLatency.Update(time.Since(startRead))
				} else {
					StateDBAccGetLatency.Update(time.Since(startRead))
				}
				d.stat.IncGet(1)
				if err != nil || value == nil {
					if err != nil {
						fmt.Println("fail to get account key", err.Error())
					}
					d.stat.IncGetNotExist(1)
				}
			}

		}(i)
	}
	wg.Wait()

	d.rDuration = time.Since(start)
	d.totalReadCost += d.rDuration

	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBGetTps.Update(int64(float64(batchSize) / float64(d.rDuration.Microseconds()) * 1000))
	} else {
		stateDBGetTps.Update(int64(float64(batchSize) / float64(d.rDuration.Microseconds()) * 1000))
	}

	start = time.Now()
	ratio := d.perfConfig.RwRatio

	// simulate update small Storage Trie
	for key, value := range taskInfo.SmallTrieTask {
		startPut := time.Now()
		// Calculate the number of elements to keep based on the ratio
		updateKeyNum := int(float64(len(value.Keys)) * ratio)

		Keys := value.Keys[:updateKeyNum]
		Vals := value.Vals[:updateKeyNum]

		// add new storage
		_, err := d.db.UpdateStorage([]byte(key), Keys, Vals)
		if err != nil {
			fmt.Println("update storage err", err.Error())
		}
		microseconds := time.Since(startPut).Microseconds() / int64(len(value.Keys))
		if d.db.GetMPTEngine() == VERSADBEngine {
			versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		} else {
			StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		}
		d.stat.IncPut(uint64(len(value.Keys)))
	}

	// simulate update large Storage Trie
	for key, value := range taskInfo.LargeTrieTask {
		// Calculate the number of elements to keep based on the ratio
		updateKeyNum := int(float64(len(value.Keys)) * ratio)

		// Create new slices based on the calculated number of elements
		newKeys := value.Keys[:updateKeyNum]
		newVals := value.Vals[:updateKeyNum]

		startPut := time.Now()
		// add new storage
		_, err := d.db.UpdateStorage([]byte(key), newKeys, newVals)
		if err != nil {
			fmt.Println("update storage err", err.Error())
		}
		microseconds := time.Since(startPut).Microseconds() / int64(len(value.Keys))
		if d.db.GetMPTEngine() == VERSADBEngine {
			versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		} else {
			StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		}
		d.stat.IncPut(uint64(len(value.Keys)))
	}

	for key, value := range taskInfo.AccountTask {
		startPut := time.Now()
		err := d.db.UpdateAccount([]byte(key), value)
		if err != nil {
			fmt.Println("update account err", err.Error())
		} else {
			d.updateAccount++
		}
		if d.db.GetMPTEngine() == VERSADBEngine {
			VersaDBAccPutLatency.Update(time.Since(startPut))
		} else {
			StateDBAccPutLatency.Update(time.Since(startPut))
		}
		d.accountKeyCache.Add(key)
		d.stat.IncPut(1)
	}

	d.wDuration = time.Since(start)
	d.totalWriteCost += d.wDuration

	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBPutTps.Update(int64(float64(batchSize) / float64(d.wDuration.Microseconds()) * 1000))
	} else {
		stateDBPutTps.Update(int64(float64(batchSize) / float64(d.wDuration.Microseconds()) * 1000))
	}

	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB := d.db.GetFlattenDB()
		for key, value := range taskInfo.AccountTask {
			// add new account
			insertKey := common.BytesToHash([]byte(key))
			rawdb.WriteAccountSnapshot(snapDB, insertKey, value)
		}

		for key, value := range taskInfo.SmallTrieTask {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}

		for key, value := range taskInfo.LargeTrieTask {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}
	}

}

func (d *DBRunner) InitSingleStorageTrie(
	key string,
	value CAKeyValue,
	firstInsert bool,
) {
	//smallStorageSize := d.perfConfig.StorageTrieSize / 100
	var snapDB ethdb.KeyValueStore
	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB = d.db.GetFlattenDB()
	}

	var err error
	v, err := d.db.GetStorage([]byte(key), []byte(value.Keys[0]))
	if err == nil && len(v) > 0 {
		panic("not inert befor")
	}
	if firstInsert {
		v, err2 := d.db.GetAccount(key)
		if err2 == nil && len(v) > 0 {
			fmt.Println("already exit the account of storage trie", key)
		}
		// add new storage
		err = d.db.AddStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("init storage err:", err.Error())
		}
	} else {
		startPut := time.Now()
		_, err = d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("update storage err:", err.Error())
		}
		microseconds := time.Since(startPut).Microseconds() / int64(len(value.Keys))
		if d.db.GetMPTEngine() == VERSADBEngine {
			versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		} else {
			StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
		}
	}

	// cache the inserted key for updating test
	if d.isLargeStorageTrie(key) {
		if len(d.largeStorageCache[key]) < 500000 {
			for i := 0; i < len(value.Keys)/500; i++ {
				d.largeStorageCache[key] = append(d.largeStorageCache[key], value.Keys[i])
			}
		}
	} else {
		if len(d.storageCache[key]) < 200000 {
			for i := 0; i < len(value.Keys)/50; i++ {
				d.storageCache[key] = append(d.storageCache[key], value.Keys[i])
			}
		}
	}

	if snapDB != nil {
		accHash := common.BytesToHash([]byte(key))
		for i, k := range value.Keys {
			rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
		}
	}
	// init 3 accounts to commit a block
	addresses, accounts := makeAccounts(1)
	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		d.db.AddAccount(initKey, accounts[i])
		d.accountKeyCache.Add(initKey)
		if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
	}

	commitStart := time.Now()
	if _, err = d.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
	d.commitDuration = time.Since(commitStart)
	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBCommitLatency.Update(d.commitDuration)
	} else {
		stateDBCommitLatency.Update(d.commitDuration)
	}
	d.trySleep()
}

func (d *DBRunner) trySleep() {
	time.Sleep(time.Duration(d.perfConfig.SleepTime) * time.Millisecond)
}

func (d *DBRunner) isLargeStorageTrie(owner string) bool {
	if owner == d.largeStorageTrie[0] || owner == d.largeStorageTrie[1] {
		return true
	}
	return false
}
