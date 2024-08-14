package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
	"github.com/pelletier/go-toml/v2"
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
	//storageCache      *lru.Cache[string, []byte]
	storageCache      map[string][]string
	largeStorageCache map[string][]string
	blockHeight       uint64
	rwDuration        time.Duration
	rDuration         time.Duration
	wDuration         time.Duration
	commitDuration    time.Duration
	hashDuration      time.Duration
	totalRwDurations  time.Duration // Accumulated rwDuration
	totalReadCost     time.Duration
	totalWriteCost    time.Duration
	BlockCount        int64 // Number of rwDuration samples
	totalHashurations time.Duration
	updateAccount     int64
	largeStorageTrie  []string
	smallStorageTrie  []string
	storageOwnerList  []string
	owners            []common.Hash
}

func NewDBRunner(
	db StateDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *DBRunner {
	runner := &DBRunner{
		db:                    db,
		stat:                  NewStat(),
		lastStatInstant:       time.Now(),
		perfConfig:            config,
		taskChan:              make(chan DBTask, taskBufferSize),
		initTaskChan:          make(chan DBTask, taskBufferSize),
		accountKeyCache:       NewFixedSizeSet(AccountKeyCacheSize),
		smallStorageTrieCache: NewFixedSizeSet(CAStorageTrieNum - 2),
		//	storageCache:    lru.NewCache[string, []byte](100000),
		storageCache:      make(map[string][]string),
		largeStorageCache: make(map[string][]string),
		largeStorageTrie:  make([]string, 2),
		smallStorageTrie:  make([]string, CAStorageTrieNum-2),
		storageOwnerList:  make([]string, CAStorageSize),
		owners:            make([]common.Hash, 10),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)
	// init the state db
	blocks := d.perfConfig.AccountsBlocks

	_, err := d.ReadConfig("config.toml")
	if err != nil {
		fmt.Println("parse config err", err.Error())
		fmt.Printf("init account in %d blocks , account num %d \n", blocks, d.perfConfig.AccountsInitSize)
		accSize := d.perfConfig.AccountsInitSize
		accBatch := d.perfConfig.AccountsBlocks
		accPerBatch := accSize / accBatch

		for i := uint64(0); i < d.perfConfig.AccountsBlocks; i++ {
			startIndex := uint64(i * accPerBatch)
			d.InitAccount(i, startIndex, accPerBatch)
		}

		ownerList := genOwnerHashKey(CAStorageTrieNum)
		for i := 0; i < CAStorageTrieNum; i++ {
			d.storageOwnerList[i] = ownerList[i]
		}
		fmt.Println("fail to load config")
		d.InitLargeStorageTasks(0)

		fmt.Println("init one large trie finish")
		d.InitLargeStorageTasks(1)

		fmt.Println("init two large trie finish")
		smallTrees := d.InitSmallStorageTasks()
		fmt.Println("init small trie finish")

		for i := uint64(0); i < d.perfConfig.AccountsBlocks; i++ {
			d.RunEmptyBlock(i)
		}
		// init the lock of each tree
		d.db.InitStorage(d.owners)
		largeTrees := make([]common.Hash, 2)
		largeTrees[0] = common.BytesToHash([]byte(d.largeStorageTrie[0]))
		largeTrees[1] = common.BytesToHash([]byte(d.largeStorageTrie[1]))

		config := &TreeConfig{largeTrees, smallTrees}
		if err = d.WriteConfig(config); err != nil {
			fmt.Println("persist config error")
		}
	} else {
		fmt.Println("load config")
		ownerList := genOwnerHashKey(CAStorageTrieNum)
		for i := 0; i < CAStorageTrieNum; i++ {
			d.storageOwnerList[i] = ownerList[i]
		}

		for i := 0; i < 2; i++ {
			owner := d.storageOwnerList[i]
			d.largeStorageTrie[i] = owner
			largeStorageInitSize := d.perfConfig.StorageTrieSize
			randomIndex := mathrand.Intn(int(d.perfConfig.StorageTrieSize / 100))
			d.largeStorageCache[string(owner)] = genStorageTrieKey(uint64(randomIndex), largeStorageInitSize/1000)
			fmt.Println("load large tree owner hash", common.BytesToHash([]byte(owner)))
		}

		for i := 0; i < CAStorageTrieNum-2; i++ {
			owner := d.storageOwnerList[i+2]
			d.smallStorageTrieCache.Add(string(owner))
			d.smallStorageTrie[i] = string(owner)
			smallStorageInitSize := d.perfConfig.StorageTrieSize / 50
			randomIndex := mathrand.Intn(int(smallStorageInitSize / 5))
			d.storageCache[owner] = genStorageTrieKey(uint64(randomIndex), smallStorageInitSize/100)
			fmt.Println("load small tree owner hash", common.BytesToHash([]byte(owner)))
		}
		/*
			largeTreeNum := len(treeConfig.LargeTrees)
			for i := 0; i < largeTreeNum; i++ {
				owner := treeConfig.LargeTrees[i].Bytes()
				d.largeStorageTrie[i] = string(owner)
				largeStorageInitSize := d.perfConfig.StorageTrieSize
				randomIndex := mathrand.Intn(int(d.perfConfig.StorageTrieSize / 100))
				d.largeStorageCache[string(owner)] = genStorageTrieKey(uint64(randomIndex), largeStorageInitSize/1000)
				fmt.Println("load large tree owner hash", d.largeStorageTrie[i])
			}
			smallTreeNum := len(treeConfig.SmallTrees)
			for i := 0; i < smallTreeNum; i++ {
				owner := treeConfig.SmallTrees[i].Bytes()
				d.smallStorageTrieCache.Add(string(owner))
				d.smallStorageTrie[i] = string(owner)
				smallStorageInitSize := d.perfConfig.StorageTrieSize / 50
				randomIndex := mathrand.Intn(int(smallStorageInitSize / 5))
				d.storageCache[string(owner)] = genStorageTrieKey(uint64(randomIndex), smallStorageInitSize/100)
				fmt.Println("load small tree owner hash", treeConfig.SmallTrees[i].String())
			}
		*/
		// init the account key cache
		accKeys := genAccountTrieKey(d.perfConfig.AccountsInitSize, AccountKeyCacheSize)
		for i := 0; i < AccountKeyCacheSize; i++ {
			d.accountKeyCache.Add(accKeys[i])
		}

	}

	fmt.Println("init db finish, begin to press kv")
	// Start task generation thread
	go d.generateRunTasks(ctx, d.perfConfig.BatchSize)

	d.runInternal(ctx)
}

func (d *DBRunner) generateRunTasks(ctx context.Context, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := NewDBTask()
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

			// small storage trie write 3/5 kv of storage
			storageUpdateNum := int(batchSize) / 5 * 3 / len(d.smallStorageTrie)
			//	StorageInitSize := d.perfConfig.StorageTrieSize
			/*
				for k, v := range d.storageCache {
					keys := make([]string, 0, storageUpdateNum)
					vals := make([]string, 0, storageUpdateNum)
					for j := 0; j < storageUpdateNum; j++ {
						// only cache 10000 for updating test
						randomIndex := mathrand.Intn(len(v))
						value := generateValue(7, 16)
						keys = append(keys, v[randomIndex])
						vals = append(vals, string(value))
					}
					taskMap.SmallStorageTask[k] = CAKeyValue{Keys: keys, Vals: vals}
				}
			*/
			for i := 0; i < len(d.smallStorageTrie); i++ {
				keys := make([]string, 0, storageUpdateNum)
				vals := make([]string, 0, storageUpdateNum)
				//owner := d.smallStorageTrie[i]
				owner := d.storageOwnerList[i+2]
				//fmt.Println("generate owner ", owner)
				v := d.storageCache[owner]
				//fmt.Println("small tree cache key len ", len(v))
				for j := 0; j < storageUpdateNum; j++ {
					// only cache 10000 for updating test
					randomIndex := mathrand.Intn(len(v))
					keys = append(keys, v[randomIndex])
					vals = append(vals, string(generateValue(7, 16)))
				}
				taskMap.SmallStorageTask[owner] = CAKeyValue{Keys: keys, Vals: vals}
			}

			// large storage trie write 1/5 kv of storage
			largeStorageUpdateNum := int(batchSize) / 5
			if len(d.largeStorageTrie) != 2 {
				panic("large tree is not 2")
			}
			// random choose one large tree to read and write
			index := mathrand.Intn(2)
			//	k := d.largeStorageTrie[index]
			k := d.storageOwnerList[index]
			v := d.largeStorageCache[k]
			//fmt.Println("large tree cache key len ", len(v))
			keys := make([]string, 0, storageUpdateNum)
			vals := make([]string, 0, storageUpdateNum)
			for j := 0; j < largeStorageUpdateNum; j++ {
				// only cache 10000 for updating test
				randomIndex := mathrand.Intn(len(v))
				value := generateValue(7, 16)
				keys = append(keys, v[randomIndex])
				vals = append(vals, string(value))
			}
			taskMap.LargeStorageTask[k] = CAKeyValue{Keys: keys, Vals: vals}

			d.taskChan <- taskMap
		}
	}
}

func (d *DBRunner) InitLargeStorageTasks(largeTrieIndex int) {
	/*
		random := mathrand.New(mathrand.NewSource(0))
		CAAccount := make([][20]byte, 1)
		for i := 0; i < 1; i++ {
			data := make([]byte, 20)
			random.Read(data)
			mathrand.Seed(time.Now().UnixNano())
			mathrand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
			copy(CAAccount[i][:], data)
		}
	*/
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
		vals := make([]string, 0, storageBatch)
		for j := uint64(0); j < storageBatch; j++ {
			value := generateValue(7, 16)
			//	keys = append(keys, string(randomStr))
			vals = append(vals, string(value))
		}
		keys := genStorageTrieKey(i*storageBatch, storageBatch)

		if i == 0 {
			d.InitSingleStorageTrie(ownerHash, CAKeyValue{
				Keys: keys, Vals: vals}, true)
		} else {
			d.InitSingleStorageTrie(ownerHash, CAKeyValue{
				Keys: keys, Vals: vals}, false)
		}
	}

	fmt.Println("init large storage trie success", "cost time", time.Since(start).Seconds(), "s")
	return
}

func (d *DBRunner) InitSmallStorageTasks() []common.Hash {

	//random := mathrand.New(mathrand.NewSource(0))
	smallTrees := make([]common.Hash, CAStorageTrieNum-2)
	/*
		CAAccount := make([][20]byte, CAStorageTrieNum-2)
		for i := 0; i < len(CAAccount); i++ {
			data := make([]byte, 20)
			random.Read(data)
			mathrand.Seed(time.Now().UnixNano())
			mathrand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
			copy(CAAccount[i][:], data)
		}

	*/

	initTrieNum := 0
	// init small tree by the config trie size
	StorageInitSize := d.perfConfig.StorageTrieSize / 50

	for i := 0; i < CAStorageTrieNum-2; i++ {
		start := time.Now()
		//	ownerHash := string(crypto.Keccak256(CAAccount[i][:]))
		ownerHash := d.storageOwnerList[i+2]
		d.smallStorageTrie[i] = ownerHash
		d.owners[i+2] = common.BytesToHash([]byte(ownerHash))
		smallTrees[i] = common.BytesToHash([]byte(ownerHash))
		blocks := d.perfConfig.TrieBlocks / 10
		fmt.Printf("init small tree in %d blocks ,  trie szie %d \n", blocks, StorageInitSize)
		storageBatch := StorageInitSize / blocks
		for t := uint64(0); t < blocks; t++ {
			vals := make([]string, 0, storageBatch)
			for j := uint64(0); j < storageBatch; j++ {
				value := generateValue(7, 16)
				vals = append(vals, string(value))
			}
			keys := genStorageTrieKey(t*storageBatch, storageBatch)
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
		fmt.Println("init storage trie success", "cost time", time.Since(start).Seconds(), "s",
			"finish trie init num", initTrieNum)

	}
	return smallTrees
}

func (r *DBRunner) runInternal(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	printAvg := 0

	for {
		select {
		case taskInfo := <-r.taskChan:
			rwStart := time.Now()
			// read, put or delete keys
			r.UpdateDB(taskInfo)
			r.rwDuration = time.Since(rwStart)
			r.totalRwDurations += r.rwDuration
			// compute hash
			hashStart := time.Now()
			r.db.Hash()
			r.hashDuration = time.Since(hashStart)
			if r.db.GetMPTEngine() == VERSADBEngine {
				VeraDBHashLatency.Update(r.hashDuration)
			} else {
				stateDBHashLatency.Update(r.hashDuration)
			}

			r.totalHashurations += r.hashDuration
			// commit
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

			// sleep 500ms for each block
			time.Sleep(500 * time.Millisecond)
			r.blockHeight++
			r.updateAccount = 0
			BlockHeight.Update(int64(r.blockHeight))
		case <-ticker.C:
			r.printStat()
			printAvg++
			if printAvg%100 == 0 {
				r.printAVGStat(startTime)
			}

		case <-ctx.Done():
			fmt.Println("Shutting down")
			r.printAVGStat(startTime)
			return
		}
	}
}

func (r *DBRunner) printAVGStat(startTime time.Time) {
	fmt.Printf(
		" Avg Perf metrics: %s, block height=%d elapsed: [read =%v us, write=%v ms, cal hash=%v us]\n",
		r.stat.CalcAverageIOStat(time.Since(startTime)),
		r.blockHeight,
		float64(r.totalReadCost.Microseconds())/float64(r.blockHeight),
		float64(r.totalWriteCost.Microseconds())/float64(r.blockHeight),
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
		err := r.db.AddAccount(initKey, accounts[i])
		if err != nil {
			fmt.Println("init account err", err)
		}
		r.accountKeyCache.Add(initKey)
		if r.db.GetMPTEngine() == StateTrieEngine && r.db.GetFlattenDB() != nil {
			// simulate insert key to snap
			snapDB := r.db.GetFlattenDB()
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
		// double write to leveldb
	}
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	r.trySleep()
	fmt.Println("init db account commit success, block number", blockNum)
}

func (r *DBRunner) RunEmptyBlock(index uint64) {
	addresses, accounts := makeAccounts(1)

	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		r.db.AddAccount(initKey, accounts[i])
		r.accountKeyCache.Add(initKey)
		if r.db.GetMPTEngine() == StateTrieEngine && r.db.GetFlattenDB() != nil {
			// simulate insert key to snap
			snapDB := r.db.GetFlattenDB()
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
		// double write to leveldb
	}

	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
	if r.db.GetMPTEngine() == VERSADBEngine {
		time.Sleep(300 * time.Millisecond)
	} else {
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("run empty block, number", index)
}

func (d *DBRunner) UpdateDB(
	taskInfo DBTask,
) {

	batchSize := int(d.perfConfig.BatchSize)
	threadNum := d.perfConfig.NumJobs
	var wg sync.WaitGroup
	start := time.Now()

	smallTrieMaps := splitTrieTask(taskInfo.SmallStorageTask, threadNum-1)

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
		for owner, CAkeys := range taskInfo.LargeStorageTask {
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
		VeraDBGetTps.Update(int64(d.perfConfig.NumJobs*batchSize) / d.rDuration.Milliseconds())
	} else {
		stateDBGetTps.Update(int64(d.perfConfig.NumJobs*batchSize) / d.rDuration.Milliseconds())
	}

	start = time.Now()

	// simulate upadte small Storage Trie
	for key, value := range taskInfo.SmallStorageTask {
		startPut := time.Now()
		// add new storage
		err := d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
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
	for key, value := range taskInfo.LargeStorageTask {
		startPut := time.Now()
		// add new storage
		err := d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
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
		VeraDBPutTps.Update(int64(batchSize) / d.wDuration.Milliseconds())
	} else {
		stateDBPutTps.Update(int64(batchSize) / d.wDuration.Milliseconds())
	}

	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB := d.db.GetFlattenDB()
		for key, value := range taskInfo.AccountTask {
			// add new account
			insertKey := common.BytesToHash([]byte(key))
			rawdb.WriteAccountSnapshot(snapDB, insertKey, value)
		}
		for key, value := range taskInfo.SmallStorageTask {
			accHash := common.BytesToHash([]byte(key))
			for i, k := range value.Keys {
				rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
			}
		}
		for key, value := range taskInfo.LargeStorageTask {
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
	smallStorageSize := d.perfConfig.StorageTrieSize / 100
	var snapDB ethdb.KeyValueStore
	if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
		// simulate insert key to snap
		snapDB = d.db.GetFlattenDB()
	}

	var err error
	if firstInsert {
		// add new storage
		err = d.db.AddStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("init storage err:", err.Error())
		}
	} else {
		err = d.db.UpdateStorage([]byte(key), value.Keys, value.Vals)
		if err != nil {
			fmt.Println("update storage err:", err.Error())
		}
	}

	// cache the inserted key for updating test
	if d.isLargeStorageTrie(key) {
		if len(d.largeStorageCache[key]) < 1500000 {
			for i := 0; i < len(value.Keys)/500; i++ {
				d.largeStorageCache[key] = append(d.largeStorageCache[key], value.Keys[i])
			}
		}
		//	d.largeStorageCache[key] = value.Keys[StorageInitSize/100 : StorageInitSize/100+StorageInitSize/100]
	} else {
		d.storageCache[key] = value.Keys[smallStorageSize/2 : smallStorageSize/2+smallStorageSize/5]
	}

	if snapDB != nil {
		accHash := common.BytesToHash([]byte(key))
		for i, k := range value.Keys {
			rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
		}
	}
	// init 3 accounts to commit a block
	addresses, accounts := makeAccounts(2)
	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		d.db.AddAccount(initKey, accounts[i])
		d.accountKeyCache.Add(initKey)
		if d.db.GetMPTEngine() == StateTrieEngine && d.db.GetFlattenDB() != nil {
			rawdb.WriteAccountSnapshot(snapDB, common.BytesToHash([]byte(initKey)), accounts[i])
		}
	}

	if _, err = d.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}

	d.trySleep()
}

func (d *DBRunner) trySleep() {
	if d.db.GetMPTEngine() == VERSADBEngine {
		time.Sleep(500 * time.Millisecond)
	} else {
		time.Sleep(100 * time.Millisecond)
	}
}

func (d *DBRunner) isLargeStorageTrie(owner string) bool {
	if owner == d.largeStorageTrie[0] || owner == d.largeStorageTrie[1] {
		return true
	}
	return false
}

func (d *DBRunner) ReadConfig(filename string) (*TreeConfig, error) {
	if _, err := os.Stat("config.toml"); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file %s does not exist", filename)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := &TreeConfig{}
	err = toml.NewDecoder(file).Decode(config)
	if err != nil {
		return nil, err
	}

	if len(config.LargeTrees) != 2 || len(config.SmallTrees) != 8 {
		return nil, fmt.Errorf("config file must contain 2 large trees and 8 small trees, but found %d large trees and %d small trees",
			len(config.LargeTrees), len(config.SmallTrees))
	}

	return config, nil
}

func (d *DBRunner) WriteConfig(config *TreeConfig) error {
	file, err := os.Create("config.toml")
	if err != nil {
		return err
	}
	defer file.Close()

	err = toml.NewEncoder(file).Encode(config)
	if err != nil {
		return err
	}

	return nil
}
