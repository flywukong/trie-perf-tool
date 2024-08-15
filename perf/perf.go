package main

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/syndtr/goleveldb/leveldb"
)

type Runner struct {
	db                 TrieDatabase
	perfConfig         PerfConfig
	stat               *Stat
	lastStatInstant    time.Time
	taskChan           chan map[string][]byte
	keyCache           *InsertedKeySet
	blockHeight        uint64
	rwDuration         time.Duration
	rDuration          time.Duration
	wDuration          time.Duration
	commitDuration     time.Duration
	hashDuration       time.Duration
	totalRwDurations   time.Duration // Accumulated rwDuration
	totalReadCost      time.Duration
	totalWriteCost     time.Duration
	BlockCount         int64 // Number of rwDuration samples
	totalHashDurations time.Duration
}

func NewRunner(
	db TrieDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *Runner {
	runner := &Runner{
		db:              db,
		stat:            NewStat(),
		lastStatInstant: time.Now(),
		perfConfig:      config,
		taskChan:        make(chan map[string][]byte, taskBufferSize),
		keyCache:        NewFixedSizeSet(1000000),
	}

	return runner
}

func (r *Runner) Run(ctx context.Context) {
	defer close(r.taskChan)
	// Start task generation thread
	go generateTasks(ctx, r.taskChan, r.perfConfig.BatchSize)

	// init the trie key
	r.InitTrie()

	fmt.Println("init trie finish, begin to press kv")
	r.runInternal(ctx)
}

func generateTasks(ctx context.Context, taskChan chan<- map[string][]byte, batchSize uint64) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			taskMap := make(map[string][]byte, batchSize)
			address, acccounts := makeAccounts(int(batchSize) / 2)
			for i := 0; i < len(address); i++ {
				taskMap[string(crypto.Keccak256(address[i][:]))] = acccounts[i]
			}
			taskChan <- taskMap
		}
	}
}

func (r *Runner) updateMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if r.db.GetMPTEngine() == VERSADBEngine {
		VeraTrieMemoryUsage.Update(int64(m.Alloc))
	} else {
		stateTrieMemoryUsage.Update(int64(m.Alloc))
	}
}

// randInt returns a random integer between min and max
func randInt(min, max int) int {
	return min + mathrand.Intn(max-min)
}

func (r *Runner) runInternal(ctx context.Context) {
	startTime := time.Now()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	printAvg := 0

	for {
		select {
		case taskInfo := <-r.taskChan:
			rwStart := time.Now()
			// read, put or delete keys
			r.UpdateTrie(taskInfo)
			r.rwDuration = time.Since(rwStart)
			r.totalRwDurations += r.rwDuration
			// compute hash
			hashStart := time.Now()
			r.db.Hash()
			r.hashDuration = time.Since(hashStart)

			if r.db.GetMPTEngine() == VERSADBEngine {
				VeraTrieHashLatency.Update(r.hashDuration)
			} else {
				stateTrieHashLatency.Update(r.hashDuration)
			}

			r.totalHashDurations += r.hashDuration
			// commit
			commitStart := time.Now()
			if r.db.GetMPTEngine() == PbssRawTrieEngine {
				if _, err := r.db.Commit(); err != nil {
					panic("failed to commit: " + err.Error())
				}
			}
			r.blockHeight++
			r.commitDuration = time.Since(commitStart)

		case <-ticker.C:
			r.printStat()
			printAvg++
			if printAvg%100 == 0 {
				r.printAVGStat(startTime)
			}
			r.updateMemoryUsage()

		case <-ctx.Done():
			fmt.Println("Shutting down")
			r.printAVGStat(startTime)
			return
		}
	}
}

func (r *Runner) printAVGStat(startTime time.Time) {
	fmt.Printf(
		" Avg Perf metrics: %s, block height=%d elapsed: [read=%v us, write=%v ms, cal hash=%v us]\n",
		r.stat.CalcAverageIOStat(time.Since(startTime)),
		r.blockHeight,
		float64(r.totalReadCost.Microseconds())/float64(r.blockHeight),
		float64(r.totalWriteCost.Microseconds())/float64(r.blockHeight),
		float64(r.totalHashDurations.Milliseconds())/float64(r.blockHeight),
	)
}

func (r *Runner) printStat() {
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

func (r *Runner) InitTrie() {
	addresses, accounts := makeAccounts(int(r.perfConfig.BatchSize) * 100)

	for i := 0; i < len(addresses); i++ {
		err := r.db.Put(crypto.Keccak256(addresses[i][:]), accounts[i])
		if err != nil {
			panic("init trie err" + err.Error())
		}
		r.keyCache.Add(string(crypto.Keccak256(addresses[i][:])))
		// double write to leveldb
		if r.db.GetFlattenDB() != nil {
			err = r.db.GetFlattenDB().Put(crypto.Keccak256(addresses[i][:]), accounts[i])
			if err != nil {
				panic("init trie err" + err.Error())
			}
		}
	}

	// commit
	if r.db.GetMPTEngine() == PbssRawTrieEngine {
		if _, err := r.db.Commit(); err != nil {
			panic("failed to commit: " + err.Error())
		}
	}
}

func (r *Runner) UpdateTrie(
	taskInfo map[string][]byte,
) {
	// todo make it as config
	batchSize := int(r.perfConfig.BatchSize)

	var wg sync.WaitGroup
	start := time.Now()
	// simulate parallel read
	for i := 0; i < r.perfConfig.NumJobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// random read of local recently cache of inserted keys
			for j := 0; j < batchSize; j++ {
				randomKey, found := r.keyCache.RandomItem()
				if found {
					keyBytes := []byte(randomKey)
					startGet := time.Now()
					var err error
					var value []byte
					if r.db.GetFlattenDB() == nil {
						value, err = r.db.Get(keyBytes)
						if err == nil {
							r.stat.IncGet(1)
							if r.db.GetMPTEngine() == VERSADBEngine {
								VeraTrieGetLatency.Update(time.Since(startGet))
							} else {
								stateTrieGetLatency.Update(time.Since(startGet))
							}
							if value == nil {
								r.stat.IncGetNotExist(1)
							}
						}
					} else {
						// if flatten db exist, read key from leveldb
						value, err = r.db.GetFlattenDB().Get(keyBytes)
						r.stat.IncGet(1)
						stateTrieGetLatency.Update(time.Since(startGet))
						if err == leveldb.ErrNotFound {
							r.stat.IncGetNotExist(1)
						}
					}

				}
			}
		}()
	}
	r.rDuration = time.Since(start)
	r.totalReadCost += r.rDuration
	wg.Wait()
	if r.db.GetMPTEngine() == VERSADBEngine {
		VeraTrieGetTps.Update(int64(r.perfConfig.NumJobs*batchSize) / r.rDuration.Microseconds())
	} else {
		stateTrieGetTps.Update(int64(r.perfConfig.NumJobs*batchSize) / r.rDuration.Microseconds())
	}

	start = time.Now()
	// simulate insert and delete trie
	for key, value := range taskInfo {
		keyName := []byte(key)
		startPut := time.Now()
		err := r.db.Put(keyName, value)
		if err != nil {
			fmt.Println("fail to insert key to trie", "key", string(keyName),
				"err", err.Error())
		}

		if r.db.GetMPTEngine() == VERSADBEngine {
			VeraTriePutLatency.Update(time.Since(startPut))
		} else {
			stateTriePutLatency.Update(time.Since(startPut))
		}
		r.keyCache.Add(key)
		r.stat.IncPut(1)
	}
	r.wDuration = time.Since(start)
	r.totalWriteCost += r.wDuration
	if r.db.GetMPTEngine() == VERSADBEngine {
		VeraTriePutTps.Update(int64(batchSize) / r.wDuration.Milliseconds())
	} else {
		stateTriePutTps.Update(int64(batchSize) / r.wDuration.Milliseconds())
	}

	// double write to leveldb
	if r.db.GetFlattenDB() != nil {
		leveldb := r.db.GetFlattenDB()
		for key, value := range taskInfo {
			keyName := []byte(key)
			err := leveldb.Put(keyName, value)
			if err != nil {
				fmt.Println("fail to insert key to trie", "key", string(keyName),
					"err", err.Error())
			}
		}
	}

	for i := 0; i < int(r.perfConfig.BatchSize); i++ {
		if randomFloat() < r.perfConfig.DeleteRatio {
			// delete the key from inserted key cache
			randomKey, found := r.keyCache.RandomItem()
			if found {
				keyName := []byte(randomKey)
				var err error
				if r.db.GetMPTEngine() == VERSADBEngine {
					err = r.db.Delete(keyName)
				} else {
					err = r.db.GetFlattenDB().Delete(keyName)
				}

				if err != nil {
					fmt.Println("fail to delete key to trie", "key", string(keyName),
						"err", err.Error())
				}
				r.stat.IncDelete(1)
			}
		}
	}
}
