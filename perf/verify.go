package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
)

type TrieVerifier struct {
	verifyDB     TrieDatabase
	baseDB       TrieDatabase
	perfConfig   PerfConfig
	taskChan     chan map[string][]byte
	blockHeight  uint64
	successBatch uint64
	failBatch    uint64
	keyCache     *InsertedKeySet
}

func NewVerifier(
	baseDB TrieDatabase,
	verifyDB TrieDatabase,
	config PerfConfig,
	taskBufferSize int, // Added a buffer size parameter for the task channel
) *TrieVerifier {
	verifier := &TrieVerifier{
		verifyDB:   verifyDB,
		baseDB:     baseDB,
		perfConfig: config,
		taskChan:   make(chan map[string][]byte, taskBufferSize),
		keyCache:   NewFixedSizeSet(1000000),
	}
	return verifier
}

func (v *TrieVerifier) Run(ctx context.Context) {
	defer close(v.taskChan)
	// Start task generation thread
	go generateTasks(ctx, v.taskChan, v.perfConfig.BatchSize)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case taskInfo := <-v.taskChan:
			v.compareHashRoot(taskInfo)
		case <-ticker.C:
			fmt.Printf(
				"[%s] verify In Progress, finish compare block %d \n",
				time.Now().Format(time.RFC3339Nano),
				v.successBatch)

		case <-ctx.Done():
			fmt.Println("Shutting down")
			return
		}
	}
}

func (v *TrieVerifier) generateTasks(running *atomic.Bool) {
	for running.Load() {
		taskMap := make(map[string][]byte)
		address, acccounts := makeAccounts(int(v.perfConfig.BatchSize))
		for i := 0; i < len(address); i++ {
			taskMap[string(crypto.Keccak256(address[i][:]))] = acccounts[i]
		}
		v.taskChan <- taskMap
	}
}

func (v *TrieVerifier) compareHashRoot(taskInfo map[string][]byte) {
	expectRoot, verifyRoot := v.getRootHash(taskInfo)
	v.blockHeight++
	if expectRoot != verifyRoot {
		fmt.Printf("compare hash root not same, pbss root %v, versa root %v \n",
			expectRoot, verifyRoot)
		v.failBatch++
		panic("unexpect compare hash root")
	} else {
		v.successBatch++
	}
}

func (v *TrieVerifier) getRootHash(taskInfo map[string][]byte) (common.Hash, common.Hash) {
	// simulate insert and delete trie
	for key, value := range taskInfo {
		keyName := []byte(key)
		err := v.baseDB.Put(keyName, value)
		if err != nil {
			fmt.Println("fail to insert key to trie", "key", string(keyName),
				"err", err.Error())
		}
		err = v.verifyDB.Put(keyName, value)
		if err != nil {
			fmt.Println("fail to insert key to trie", "key", string(keyName),
				"err", err.Error())
		}
		log.Info("insert key", "key", keyName)
		v.keyCache.Add(key)

		if randomFloat() < v.perfConfig.DeleteRatio {
			// delete the key from inserted key cache
			randomKey, found := v.keyCache.RandomItem()
			if found {
				keyName = []byte(randomKey)
				err = v.baseDB.Delete(keyName)
				if err != nil {
					fmt.Println("fail to delete key to trie", "key", string(keyName),
						"err", err.Error())
				}
				err = v.verifyDB.Delete(keyName)
				if err != nil {
					fmt.Println("fail to delete key to trie", "key", string(keyName),
						"err", err.Error())
				}
				log.Info("delete key", "key", keyName)
			}
		}
	}

	baseRoot, err := v.baseDB.Commit()
	if err != nil {
		panic("state trie commit error")
	}
	verifyRoot := v.verifyDB.Hash()

	return baseRoot, verifyRoot
}
