package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
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
	storageCache          map[common.Address][]string
	largeStorageCache     map[common.Address][]string
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
	largeStorageTrie      []common.Address
	smallStorageTrie      []common.Address
	storageOwnerList      []common.Address
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
		storageCache:          make(map[common.Address][]string),
		largeStorageCache:     make(map[common.Address][]string),
		largeStorageTrie:      make([]common.Address, largeTrieNum),
		smallStorageTrie:      make([]common.Address, totalTrieNum-largeTrieNum),
		storageOwnerList:      make([]common.Address, totalTrieNum+MaxLargeStorageTrieNum),
		owners:                make([]common.Hash, totalTrieNum+MaxLargeStorageTrieNum),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)

	// init the state db
	blocks := d.perfConfig.AccountsBlocks
	totalTrieNum := int(d.perfConfig.StorageTrieNum)
	fmt.Println("init storage trie number:", totalTrieNum)

	largeTrieNum := int(d.perfConfig.LargeTrieNum)

	if d.perfConfig.IsInitMode {
		//	debug.SetGCPercent(80)
		//	debug.SetMemoryLimit(48 * 1024 * 1024 * 1024)

		fmt.Printf("init account in %d blocks , account num %d \n", blocks, d.perfConfig.AccountsInitSize)
		diskVersion := d.db.GetVersion()
		fmt.Println("disk version is", diskVersion)

		/*
			accSize := d.perfConfig.AccountsInitSize
			accBatch := d.perfConfig.AccountsBlocks
			accPerBatch := accSize / accBatch


			for i := uint64(0); i < d.perfConfig.AccountsBlocks; i++ {
				startIndex := i * accPerBatch
				d.InitAccount(i, startIndex, accPerBatch)
			}

		*/

		// generate the storage owners, the first two owner is the large storage and
		// the others are small trie
		ownerList := genOwnerHashKeyV2(totalTrieNum + MaxLargeStorageTrieNum)
		for i := 0; i < totalTrieNum+MaxLargeStorageTrieNum; i++ {
			d.storageOwnerList[i] = common.BytesToAddress(ownerList[i])
		}

		//	d.InitLargeStorageTries()
		fmt.Println("init the large tries finish")
		d.db.RepairSnap(d.storageOwnerList, int(d.perfConfig.LargeTrieNum))
		d.InitSmallStorageTrie()
		fmt.Println("init small trie finish")

		// init the lock of each tree
		d.db.InitStorage(d.owners, totalTrieNum+MaxLargeStorageTrieNum)
		d.updateCache(uint64(largeTrieNum), uint64(totalTrieNum))
	} else {
		fmt.Println("reload the db and restart press test")
		ownerList := genOwnerHashKeyV2(totalTrieNum + MaxLargeStorageTrieNum)
		for i := 0; i < totalTrieNum+MaxLargeStorageTrieNum; i++ {
			d.storageOwnerList[i] = common.BytesToAddress(ownerList[i])
			d.owners[i] = crypto.Keccak256Hash(d.storageOwnerList[i].Bytes())
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
	var wg sync.WaitGroup

	wg.Add(1)

	/*
		go func() {
			defer wg.Done()
			for i := uint64(0); i < largeTrieNum; i++ {
				owner := d.storageOwnerList[i]
				d.largeStorageTrie[i] = owner
				ownerHash := crypto.Keccak256Hash(owner.Bytes())
				largeStorageInitSize := d.perfConfig.StorageTrieSize

				index := mathrand.Intn(5)
				startRange := (int(d.perfConfig.StorageTrieSize) / 5 * index)
				endRange := (int(d.perfConfig.StorageTrieSize) / 5 * (index + 1))
				middleRangeStart := startRange + (endRange-startRange)/4
				middleRangeEnd := endRange - (endRange-startRange)/4
				randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)

				d.largeStorageCache[owner] = genStorageTrieKey(ownerHash, uint64(randomIndex), largeStorageInitSize/1000)
			}
			fmt.Println("update large storage cache finish")
		}()

	*/

	go func() {
		defer wg.Done()
		for i := uint64(0); i < totalTrieNum-largeTrieNum; i++ {
			owner := d.storageOwnerList[i+MaxLargeStorageTrieNum]
			d.smallStorageTrie[i] = owner
			ownerHash := crypto.Keccak256Hash(owner.Bytes())
			smallStorageInitSize := d.perfConfig.SmallStorageSize

			index := mathrand.Intn(5)
			startRange := (int(smallStorageInitSize) / 5 * index)
			endRange := (int(smallStorageInitSize) / 5 * (index + 1))
			middleRangeStart := startRange + (endRange-startRange)/4
			middleRangeEnd := endRange - (endRange-startRange)/4

			randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)

			d.storageCache[owner] = genStorageTrieKey(ownerHash, uint64(randomIndex), smallStorageInitSize/2000)
		}
		fmt.Println("update small storage cache finish")
	}()

	/*
		go func() {
			defer wg.Done()
			// init the account key cache
			adddresses := genAccountKeyV2(d.perfConfig.AccountsInitSize, AccountKeyCacheSize)
			for i := 0; i < AccountKeyCacheSize; i++ {
				//	d.accountKeyCache.Add(accKeys[i])
				d.accountKeyCache.Add(string(adddresses[i][:]))
			}
			fmt.Println("update account cache finish")
		}()

	*/

	wg.Wait()
}

func (d *DBRunner) generateRunTasks(ctx context.Context, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := NewDBTask()
			var wg sync.WaitGroup
			wg.Add(1)

			/*
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

					accountList := genAccountKeyV2(d.perfConfig.AccountsInitSize, uint64(updateAccounts)/10*6)

					for i := 0; i < updateAccounts/10*5; i++ {
						//	d.accountKeyCache.Add(accKeys[i])
						acc := new(types.StateAccount)
						err := rlp.DecodeBytes(accounts[i], acc)
						if err != nil {
							fmt.Printf("Failed to decode RLP")
						}
						taskMap.AccountTask[common.BytesToAddress(accountList[i][:])] = acc
					}

					for i := 0; i < updateAccounts/10*5; i++ {
						randomkey, exist := d.accountKeyCache.RandomItem()
						if exist {
							acc := new(types.StateAccount)
							err := rlp.DecodeBytes(accounts[i], acc)
							if err != nil {
								fmt.Printf("Failed to decode RLP")
							}
							task.AccountTask[common.BytesToAddress([]byte(randomkey))] = acc
							//	fmt.Println("set taskMap address", common.BytesToAddress(accountList[i][:]).String())
						} else {
							fmt.Println("fail to find test account in cache")
						}
					}
				}(&taskMap)
			*/
			min_value_size := d.perfConfig.MinValueSize
			max_value_size := d.perfConfig.MaxValueSize

			go func(task *DBTask) {
				defer wg.Done()
				smallTrieTestData := make(map[common.Address][]string)
				// small storage trie write 3/5 kv of storage
				var randomStorageTrieList []common.Address
				// random choose 29 small tries
				if len(d.smallStorageTrie) > SmallTriesReadInBlock {
					randomStorageTrieList = make([]common.Address, SmallTriesReadInBlock)
					perm := mathrand.Perm(len(d.smallStorageTrie))
					for i := 0; i < SmallTriesReadInBlock; i++ {
						randomStorageTrieList[i] = d.smallStorageTrie[perm[i]]
					}
				} else {
					randomStorageTrieList = d.smallStorageTrie
				}

				smallStorageInitSize := d.perfConfig.SmallStorageSize

				storageUpdateNum := int(batchSize) / 5 * 3 / len(randomStorageTrieList)
				for i := 0; i < len(randomStorageTrieList); i++ {
					owner := randomStorageTrieList[i]
					index := mathrand.Intn(5)
					startRange := (int(smallStorageInitSize) / 5 * index)
					endRange := (int(smallStorageInitSize) / 5 * (index + 1))
					middleRangeStart := startRange + (endRange-startRange)/4
					middleRangeEnd := endRange - (endRange-startRange)/4
					randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)
					ownerHash := crypto.Keccak256Hash(owner.Bytes())
					smallTrieTestData[owner] = genStorageTrieKey(ownerHash, uint64(randomIndex), uint64(storageUpdateNum))
				}

				for i := 0; i < len(randomStorageTrieList); i++ {
					keys := make([]string, 0, storageUpdateNum)
					vals := make([]string, 0, storageUpdateNum)
					owner := randomStorageTrieList[i]
					//	owner := d.storageOwnerList[i+MaxLargeStorageTrieNum]
					v := smallTrieTestData[owner]
					for j := 0; j < storageUpdateNum/10*5; j++ {
						// only cache 10000 for updating test
						randomIndex := mathrand.Intn(len(v))
						keys = append(keys, v[randomIndex])
						vals = append(vals, string(generateValueV2(min_value_size, max_value_size)))
					}

					v2 := d.storageCache[owner]
					for j := 0; j < storageUpdateNum/10*5; j++ {
						// only cache 10000 for updating test
						randomIndex := mathrand.Intn(len(v2))
						keys = append(keys, v2[randomIndex])
						vals = append(vals, string(generateValueV2(min_value_size, max_value_size)))
					}

					task.SmallTrieTask[owner] = CAKeyValue{Keys: keys, Vals: vals}
				}
			}(&taskMap)
			/*
				go func(task *DBTask) {
					defer wg.Done()
					// large storage trie write 1/5 kv of storage
					largeStorageUpdateNum := int(batchSize) / 5
					largeTrieNum := int(d.perfConfig.LargeTrieNum)
					if len(d.largeStorageTrie) != largeTrieNum {
						panic("large tree is not right")
					}
					// random choose one large tree to read and write
					index := mathrand.Intn(largeTrieNum)
					//	k := d.largeStorageTrie[index]
					address := d.storageOwnerList[index]
					largeStorageCache := make(map[common.Address][]string)

					id := mathrand.Intn(5)
					startRange := (int(d.perfConfig.StorageTrieSize) / 5 * id)
					endRange := (int(d.perfConfig.StorageTrieSize) / 5 * (id + 1))
					middleRangeStart := startRange + (endRange-startRange)/4
					middleRangeEnd := endRange - (endRange-startRange)/4
					randomIndex := middleRangeStart + mathrand.Intn(middleRangeEnd-middleRangeStart)

					ownerHash := crypto.Keccak256Hash(address.Bytes())
					largeStorageCache[address] = genStorageTrieKey(ownerHash, uint64(randomIndex), uint64(largeStorageUpdateNum))

					v := largeStorageCache[address]
					//fmt.Println("large tree cache key len ", len(v))
					keys := make([]string, 0, largeStorageUpdateNum)
					vals := make([]string, 0, largeStorageUpdateNum)
					for j := 0; j < largeStorageUpdateNum/10*5; j++ {
						// only cache 10000 for updating test
						randomIndex = mathrand.Intn(len(v))
						value := generateValueV2(min_value_size, max_value_size)
						keys = append(keys, v[randomIndex])
						vals = append(vals, string(value))
					}

					v2 := d.largeStorageCache[address]
					for j := 0; j < largeStorageUpdateNum/10*5; j++ {
						// only cache 10000 for updating test
						randomIndex = mathrand.Intn(len(v2))
						value := generateValueV2(min_value_size, max_value_size)
						keys = append(keys, v2[randomIndex])
						vals = append(vals, string(value))
					}

					task.LargeTrieTask[address] = CAKeyValue{Keys: keys, Vals: vals}
				}(&taskMap)
			*/
			d.taskChan <- taskMap
		}
	}
}

func (d *DBRunner) InitLargeStorageTrie(largeTrieIndex int) {
	StorageInitSize := d.perfConfig.StorageTrieSize
	start := time.Now()
	//	ownerHash := string(crypto.Keccak256(CAAccount[0][:]))
	address := d.storageOwnerList[largeTrieIndex]
	ownerHash := crypto.Keccak256Hash(address.Bytes())
	d.largeStorageTrie[largeTrieIndex] = address
	d.owners[largeTrieIndex] = ownerHash
	fmt.Println("large trie owner hash", ownerHash)
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

		if i == 0 {
			d.InitSingleStorageTrie(address, CAKeyValue{
				Keys: keys, Vals: vals}, true)
		} else {
			d.InitSingleStorageTrie(address, CAKeyValue{
				Keys: keys, Vals: vals}, false)
		}
	}

	fmt.Println("init large storage trie success", "cost time", time.Since(start).Seconds(), "s")
	return
}

func (d *DBRunner) InitLargeStorageTries() {
	largeTrieNum := int(d.perfConfig.LargeTrieNum)
	for i := 0; i < largeTrieNum; i++ {
		d.InitLargeStorageTrie(i)
		fmt.Printf("init the  %d large trie success \n", i+1)
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

	for i := 105; i < int(CATrieNum-d.perfConfig.LargeTrieNum); i++ {
		address := d.storageOwnerList[i+MaxLargeStorageTrieNum]
		ownerHash := crypto.Keccak256Hash(address.Bytes())
		//ownerHash := d.storageOwnerList[i+MaxLargeStorageTrieNum]
		fmt.Println("generate small trie, owner:", ownerHash, "address", address.String())
		d.smallStorageTrie[i] = address
		d.owners[i+MaxLargeStorageTrieNum] = ownerHash

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
				fmt.Println("first init")
				d.InitSingleStorageTrie(address, CAKeyValue{
					Keys: keys, Vals: vals}, true)
			} else {
				d.InitSingleStorageTrie(address, CAKeyValue{
					Keys: keys, Vals: vals}, false)
			}
		}

		initTrieNum++
		fmt.Printf("init small tree in %d blocks, trie szie %d, hash %v, finish trie init num %d \n",
			blocks, StorageInitSize, ownerHash, initTrieNum)

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
			if d.blockHeight > 3000 {
				return
			}
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
		//initKey := string(crypto.Keccak256(addresses[i][:]))
		address := common.BytesToAddress(addresses[i][:])
		startPut := time.Now()
		err := r.db.AddAccount(address, accounts[i])
		if err != nil {
			fmt.Println("init account err", err)
		}
		if r.db.GetMPTEngine() == VERSADBEngine {
			VersaDBAccPutLatency.Update(time.Since(startPut))
		} else {
			StateDBAccPutLatency.Update(time.Since(startPut))
		}
		r.accountKeyCache.Add(address.String())
		if r.db.GetMPTEngine() == StateTrieEngine && r.db.GetFlattenDB() != nil {
			// simulate insert key to snap
			snapDB := r.db.GetFlattenDB()

			data, err := rlp.EncodeToBytes(accounts[i])
			if err != nil {
				fmt.Println("decode account err when init")
			}
			rawdb.WriteAccountSnapshot(snapDB, crypto.Keccak256Hash(address.Bytes()), data)
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

	var snapDB ethdb.KeyValueStore
	var cache *fastcache.Cache
	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil && d.db.GetCache() != nil {
		// simulate insert key to snap
		snapDB = d.db.GetFlattenDB()
		cache = d.db.GetCache()
	}
	batchSize := int(d.perfConfig.BatchSize)
	var wg sync.WaitGroup
	start := time.Now()

	var address []common.Address

	for addr, _ := range taskInfo.SmallTrieTask {
		address = append(address, addr)
	}
	for addr, _ := range taskInfo.LargeTrieTask {
		address = append(address, addr)
	}

	if d.blockHeight%100 == 0 {
		fmt.Println("address list len", len(address))
	}
	err := d.db.OpenStorageTries(address)
	if err != nil {
		fmt.Println("opne storage trie err", err.Error())
	}
	microseconds := time.Since(start).Microseconds() / int64(len(address))
	if d.db.GetMPTEngine() == VERSADBEngine {
		VersaDBOpenTreeLatency.Update(time.Duration(microseconds) * time.Microsecond)
	} else {
		StateDBOpenTreeLatency.Update(time.Duration(microseconds) * time.Microsecond)
	}

	threadNum := d.perfConfig.NumJobs

	start = time.Now()
	smallTrieMaps := splitTrieTask(taskInfo.SmallTrieTask, threadNum-1)

	for i := 0; i < threadNum-1; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			for owner, CAKeys := range smallTrieMaps[index] {
				for j := 0; j < len(CAKeys.Keys); j++ {
					startRead := time.Now()
					value, err := d.db.GetStorage(owner, []byte(CAKeys.Keys[j]))
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
						fmt.Println("fail to get small trie key")
						d.stat.IncGetNotExist(1)
					}
				}
			}
		}(i)
	}

	/*
		// use one thread to read a random large storage trie
		wg.Add(1)
		go func() {
			defer wg.Done()
			for owner, CAkeys := range taskInfo.LargeTrieTask {
				for i := 0; i < len(CAkeys.Keys); i++ {
					startRead := time.Now()
					value, err := d.db.GetStorage(owner, []byte(CAkeys.Keys[i]))
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
						fmt.Println("fail to get large tree key")
						d.stat.IncGetNotExist(1)
					}
				}
			}
		}()
	*/
	wg.Wait()

	/*
		//	fmt.Println("account task key num", len(taskInfo.AccountTask), "height", d.blockHeight)
		accountMaps := splitAccountTask(taskInfo.AccountTask, threadNum)
		//  read 1/5 kv of account
		for i := 0; i < threadNum; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				//		fmt.Println("account map key num:", len(accountMaps[index]), "height", d.blockHeight
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
						fmt.Println("fail to get account key")
						d.stat.IncGetNotExist(1)
					} else {
						accHash := crypto.Keccak256Hash(key.Bytes())
						data, err := rlp.EncodeToBytes(value)
						if err != nil {
							fmt.Println("decode account err when init")
						}
						if d.db.GetMPTEngine() == StateTrieEngine {
							rawdb.WriteAccountSnapshot(snapDB, accHash, data)
							cache.Set(accHash[:], data)
						}
					}
				}
			}(i)
		}
		wg.Wait()

	*/

	d.rDuration = time.Since(start)
	d.totalReadCost += d.rDuration

	if d.db.GetMPTEngine() == VERSADBEngine {
		VeraDBGetTps.Update(int64(float64(batchSize) / float64(d.rDuration.Microseconds()) * 1000))
	} else {
		stateDBGetTps.Update(int64(float64(batchSize) / float64(d.rDuration.Microseconds()) * 1000))
	}

	wg.Add(2)

	go func() {
		start = time.Now()
		ratio := d.perfConfig.RwRatio
		defer wg.Done()
		// simulate update small Storage Trie
		for owner, value := range taskInfo.SmallTrieTask {
			startPut := time.Now()
			// Calculate the number of elements to keep based on the ratio
			updateKeyNum := int(float64(len(value.Keys)) * ratio)

			Keys := value.Keys[:updateKeyNum]
			Vals := value.Vals[:updateKeyNum]
			// add new storage
			_, err := d.db.UpdateStorage(owner, Keys, Vals)
			if err != nil {
				fmt.Println("update storage err", err.Error())
			}
			microseconds := time.Since(startPut).Microseconds() / int64(len(Keys))
			if d.db.GetMPTEngine() == VERSADBEngine {
				versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
			} else {
				StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
			}
			d.stat.IncPut(uint64(len(Keys)))
		}

		// simulate update large Storage Trie
		for owner, value := range taskInfo.LargeTrieTask {
			// Calculate the number of elements to keep based on the ratio
			updateKeyNum := int(float64(len(value.Keys)) * ratio)

			// Create new slices based on the calculated number of elements
			newKeys := value.Keys[:updateKeyNum]
			newVals := value.Vals[:updateKeyNum]

			startPut := time.Now()
			// add new storage
			_, err := d.db.UpdateStorage(owner, newKeys, newVals)
			if err != nil {
				fmt.Println("update storage err", err.Error())
			}
			microseconds = time.Since(startPut).Microseconds() / int64(len(newKeys))
			if d.db.GetMPTEngine() == VERSADBEngine {
				versaDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
			} else {
				StateDBStoragePutLatency.Update(time.Duration(microseconds) * time.Microsecond)
			}
			d.stat.IncPut(uint64(len(newKeys)))
		}

		updateKeyNum := int(float64(len(taskInfo.AccountTask)) * ratio)
		num := 0
		for key, value := range taskInfo.AccountTask {
			num++
			if num > updateKeyNum {
				break
			}
			startPut := time.Now()
			err := d.db.UpdateAccount(key, value)
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
			d.stat.IncPut(1)
		}

		d.wDuration = time.Since(start)
		d.totalWriteCost += d.wDuration

		if d.db.GetMPTEngine() == VERSADBEngine {
			VeraDBPutTps.Update(int64(float64(batchSize) * d.perfConfig.RwRatio / float64(d.wDuration.Microseconds()) * 1000))
		} else {
			stateDBPutTps.Update(int64(float64(batchSize) * d.perfConfig.RwRatio / float64(d.wDuration.Microseconds()) * 1000))
		}
	}()

	go func() {
		defer wg.Done()
		ratio := d.perfConfig.RwRatio
		if d.db.GetMPTEngine() == StateTrieEngine && snapDB != nil && cache != nil {
			start2 := time.Now()
			defer func() {
				if d.blockHeight%100 == 0 {
					fmt.Println("update snap cost:", time.Since(start2).Milliseconds(), "ms")
				}
			}()

			// simulate insert key to snap
			for key, value := range taskInfo.AccountTask {
				// add new account
				accHash := crypto.Keccak256Hash(key.Bytes())
				data, err := rlp.EncodeToBytes(value)
				if err != nil {
					fmt.Println("decode account err when init")
				}
				rawdb.WriteAccountSnapshot(snapDB, accHash, data)
				cache.Set(accHash[:], data)
			}

			for key, value := range taskInfo.SmallTrieTask {
				accHash := crypto.Keccak256Hash(key.Bytes())
				updateKeyNum := int(float64(len(value.Keys)) * ratio)

				// Create new slices based on the calculated number of elements
				newKeys := value.Keys[:updateKeyNum]
				newVals := value.Vals[:updateKeyNum]
				for i, k := range newKeys {
					storageHash := hashData([]byte(k))
					cachekey := append(accHash[:], storageHash[:]...)
					rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(newVals[i]))
					cache.Set(cachekey, []byte(newVals[i]))
				}
			}

			for key, value := range taskInfo.LargeTrieTask {
				accHash := crypto.Keccak256Hash(key.Bytes())
				updateKeyNum := int(float64(len(value.Keys)) * ratio)

				// Create new slices based on the calculated number of elements
				newKeys := value.Keys[:updateKeyNum]
				newVals := value.Vals[:updateKeyNum]
				for i, k := range newKeys {
					storageHash := hashData([]byte(k))
					cachekey := append(accHash[:], storageHash[:]...)
					rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(newVals[i]))
					cache.Set(cachekey, []byte(newVals[i]))
				}
			}
		}
	}()

	wg.Wait()
}

func (d *DBRunner) InitSingleStorageTrie(
	address common.Address,
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

	if firstInsert {
		v, err2 := d.db.GetAccount(address)
		if err2 == nil && len(v) > 0 {
			fmt.Println("already exit the account of storage trie",
				crypto.Keccak256Hash(address.Bytes()), "address", address.String())
		}
		// add new storage
		err = d.db.AddStorage(address, value.Keys, value.Vals)
		if err != nil {
			fmt.Println("init storage err:", err.Error())
		}
	} else {
		var addresses []common.Address
		addresses = append(addresses, address)
		err = d.db.OpenStorageTries(addresses)
		if err != nil {
			fmt.Println("opne storage trie err", err.Error())
		}
		startPut := time.Now()
		_, err = d.db.UpdateStorage(address, value.Keys, value.Vals)
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

	/*
		// cache the inserted key for updating test
		if d.isLargeStorageTrie(address) {
			if len(d.largeStorageCache[address]) < 500000 {
				for i := 0; i < len(value.Keys)/500; i++ {
					d.largeStorageCache[address] = append(d.largeStorageCache[address], value.Keys[i])
				}
			}
		} else {
			if len(d.storageCache[address]) < 200000 {
				for i := 0; i < len(value.Keys)/50; i++ {
					d.storageCache[address] = append(d.storageCache[address], value.Keys[i])
				}
			}
		}

	*/

	if snapDB != nil {
		accHash := crypto.Keccak256Hash(address.Bytes())
		for i, k := range value.Keys {
			rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
		}
	}
	// init 3 accounts to commit a block
	addresses, accounts := makeAccountsV3(2)
	for i := 0; i < len(addresses); i++ {
		addressName := common.BytesToAddress(addresses[i][:])
		d.db.AddAccount(addressName, accounts[i])
		d.accountKeyCache.Add(addressName.String())

		if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
			data, err := rlp.EncodeToBytes(accounts[i])
			if err != nil {
				fmt.Println("decode account err when init")
			}
			rawdb.WriteAccountSnapshot(snapDB, crypto.Keccak256Hash(addressName.Bytes()), data)
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

func (d *DBRunner) isLargeStorageTrie(owner common.Address) bool {
	if owner == d.largeStorageTrie[0] || owner == d.largeStorageTrie[1] {
		return true
	}
	return false
}
