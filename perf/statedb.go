package main

import (
	"github.com/ethereum/go-ethereum/common"
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
	GetAccount(string) ([]byte, error)

	AddAccount(key string, value []byte) error

	AddStorage(owner []byte, keys []string, vals []string) error

	GetStorage(owner []byte, key []byte) ([]byte, error)

	//	UpdateStorage(owner []byte, keys []byte, value []byte) error

	Commit() (common.Hash, error)

	Hash() common.Hash

	GetMPTEngine() string

	GetFlattenDB() ethdb.KeyValueStore
}

type TrieBatch interface {
	Put(key []byte, val []byte) error
	Del(key []byte) error
}
