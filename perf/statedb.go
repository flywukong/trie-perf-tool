package main

import (
	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
)

// https://github.com/bnb-chain/versioned-state-database/blob/develop/db.go

// Database wraps access to tries and contract code.
type TrieDatabase interface {
	//OpenDB(string) TrieDatabase
	Put(key []byte, value []byte) error // insert single key value

	Get(key []byte) ([]byte, error)

	Delete(key []byte) error

	Commit() (common.Hash, error)

	Hash() common.Hash

	GetMPTEngine() string

	GetFlattenDB() ethdb.KeyValueStore
}

type StateDatabase interface {
	GetAccount(address common.Address) ([]byte, error)
	GetAccountFromTrie(address common.Address) ([]byte, error)

	AddAccount(address common.Address, acc *types.StateAccount) error

	UpdateAccount(address common.Address, acc *types.StateAccount) error

	AddStorage(address common.Address, keys []string, vals []string) error

	GetStorage(address common.Address, key []byte) ([]byte, error)

	GetStorageFromTrie(address common.Address, key []byte) ([]byte, error)

	UpdateStorage(address common.Address, keys []string, value []string) (common.Hash, error)

	Commit() (common.Hash, error)

	Hash() common.Hash

	GetMPTEngine() string

	GetFlattenDB() ethdb.KeyValueStore

	InitStorage(owners []common.Hash, trieNum int)

	RepairSnap(owners []common.Address, trieNum int)

	GetVersion() int64

	GetCache() *fastcache.Cache

	OpenStorageTries(addresses []common.Address) error
}

type TrieBatch interface {
	Put(key []byte, val []byte) error
	Del(key []byte) error
}
