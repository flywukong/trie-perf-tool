package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
)

type DBRunner struct {
	db                StateDatabase
	perfConfig        PerfConfig
	stat              *Stat
	lastStatInstant   time.Time
	taskChan          chan DBTask
	keyCache          *InsertedKeySet
	ownerCache        *InsertedKeySet
	storageCache      *lru.Cache[string, []byte]
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
}

func NewDBRunner(
	db StateDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *DBRunner {
	runner := &DBRunner{
		db:              db,
		stat:            NewStat(),
		lastStatInstant: time.Now(),
		perfConfig:      config,
		taskChan:        make(chan DBTask, taskBufferSize),
		keyCache:        NewFixedSizeSet(100000),
		ownerCache:      NewFixedSizeSet(100000),
		storageCache:    lru.NewCache[string, []byte](100000),
	}

	return runner
}

func (d *DBRunner) Run(ctx context.Context) {
	defer close(d.taskChan)
	// Start task generation thread
	go generateStorageTasks(ctx, d.taskChan, d.perfConfig.BatchSize/2)
	// init the state db
	d.InitDB()

	fmt.Println("init db finish, begin to press kv")
	d.runInternal(ctx)
}

func generateStorageTasks(ctx context.Context, taskChan chan<- DBTask, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:

			taskMap := make(DBTask, batchSize)
			random := mathrand.New(mathrand.NewSource(0))

			address, accounts := makeAccounts(int(batchSize) - CAStorageNum)
			for i := 0; i < len(address); i++ {
				taskMap[string(crypto.Keccak256(address[i][:]))] = CAKeyValue{
					Account: accounts[i]}
			}

			CAAccount := make([][20]byte, CAStorageNum)
			for i := 0; i < len(CAAccount); i++ {
				data := make([]byte, 20)
				random.Read(data)
				mathrand.Seed(time.Now().UnixNano())
				mathrand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
				copy(CAAccount[i][:], data)
			}

			keys := make([]string, 0, CAStorageSize)
			vals := make([]string, 0, CAStorageSize)

			for i := 0; i < len(CAAccount); i++ {
				for j := 0; j < CAStorageSize; j++ {
					randomStr := generateValue(32, 32)
					//	randomHash := common.BytesToHash(randomStr)
					//	path := generateValue(0, 64)
					//	key := string(storageTrieNodeKey(randomHash, path))
					value := generateValue(7, 16)
					keys = append(keys, string(randomStr))
					vals = append(vals, string(value))
				}
				taskMap[string(crypto.Keccak256(CAAccount[i][:]))] = CAKeyValue{
					Keys: keys, Vals: vals}
			}

			taskChan <- taskMap
		}
	}
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

			r.blockHeight++
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

func (r *DBRunner) InitDB() {
	addresses, accounts := makeAccounts(int(r.perfConfig.BatchSize) * 100)

	for i := 0; i < len(addresses); i++ {
		r.db.AddAccount(string(crypto.Keccak256(addresses[i][:])), accounts[i])
		r.keyCache.Add(string(crypto.Keccak256(addresses[i][:])))
		// double write to leveldb
	}

	fmt.Println("init db account suceess")
	if _, err := r.db.Commit(); err != nil {
		panic("failed to commit: " + err.Error())
	}
}

func (d *DBRunner) UpdateDB(
	taskInfo DBTask,
) {
	// todo make it as config
	batchSize := int(d.perfConfig.BatchSize)
	threadNum := d.perfConfig.NumJobs
	var needReadStorage bool
	var wg sync.WaitGroup
	start := time.Now()
	// simulate parallel read
	readStorageSet, err := d.ownerCache.GetNRandomSets(threadNum, batchSize/2)
	if err == nil {
		needReadStorage = true
	}
	for i := 0; i < threadNum; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if needReadStorage {
				for j := 0; j < batchSize/2; j++ {
					startRead := time.Now()
					storageKey := readStorageSet[index][j]
					if key, exist := d.storageCache.Get(storageKey); exist {
						value, err := d.db.GetStorage([]byte(storageKey), key)
						if d.db.GetMPTEngine() == VERSADBEngine {
							versaDBStorageGetLatency.Update(time.Since(startRead))
						} else {
							StateDBStorageGetLatency.Update(time.Since(startRead))
						}
						d.stat.IncGet(1)
						if err != nil || value == nil {
							if err != nil {
								fmt.Println("fail to get key", err.Error())
							}
							d.stat.IncGetNotExist(1)
						}
					}
				}
			}
			// random read of local recently cache of inserted keys
			for j := 0; j < batchSize/2; j++ {
				randomKey, found := d.keyCache.RandomItem()
				if found {
					var value []byte
					startRead := time.Now()
					value, err := d.db.GetAccount(randomKey)
					if d.db.GetMPTEngine() == VERSADBEngine {
						VersaDBAccGetLatency.Update(time.Since(startRead))
					} else {
						StateDBAccGetLatency.Update(time.Since(startRead))
					}
					d.stat.IncGet(1)
					if err != nil || value == nil {
						if err != nil {
							fmt.Println("fail to get key", err.Error())
						}
						d.stat.IncGetNotExist(1)
					}
				} else {
					fmt.Println("fail to get account key")
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
	// simulate insert Account and Storage Trie
	for key, value := range taskInfo {
		startPut := time.Now()
		if len(value.Account) > 0 {
			// add new account
			d.db.AddAccount(key, value.Account)
			if d.db.GetMPTEngine() == VERSADBEngine {
				VersaDBAccPutLatency.Update(time.Since(startPut))
			} else {
				StateDBAccPutLatency.Update(time.Since(startPut))
			}
			d.keyCache.Add(key)
		} else {
			// add new storage
			d.db.AddStorage([]byte(key), value.Keys, value.Vals)
			if d.db.GetMPTEngine() == VERSADBEngine {
				versaDBStoragePutLatency.Update(time.Since(startPut))
			} else {
				StateDBStoragePutLatency.Update(time.Since(startPut))
			}
			d.storageCache.Add(key, []byte(value.Keys[2]))
			d.ownerCache.Add(key)
		}
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
		for key, value := range taskInfo {
			if len(value.Account) > 0 {
				// add new account
				insertKey := common.BytesToHash([]byte(key))
				rawdb.WriteAccountSnapshot(snapDB, insertKey, value.Account)
			} else {
				// add new storage
				accHash := common.BytesToHash([]byte(key))
				for i, k := range value.Keys {
					rawdb.WriteStorageSnapshot(snapDB, accHash, hashData([]byte(k)), []byte(value.Vals[i]))
				}
			}
			d.stat.IncPut(1)
		}
	}
}
