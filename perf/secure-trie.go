package main

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	bsctrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
)

type PbssStateTrie struct {
	trie      *bsctrie.StateTrie
	db        *triedb.Database
	nodes     *trienode.MergedNodeSet
	flattenDB ethdb.KeyValueStore
}

func OpenStateTrie(dataDir string, root common.Hash) *PbssStateTrie {
	triedb, _ := MakePBSSTrieDatabase(dataDir)

	t, err := bsctrie.NewStateTrie(bsctrie.TrieID(root), triedb)
	if err != nil {
		panic("create state trie err")
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}

	leveldb, err := rawdb.NewLevelDBDatabase("leveldb", 1000, 20000, "", false)
	if err != nil {
		panic("create leveldb err")
	}

	return &PbssStateTrie{
		trie:      t,
		db:        triedb,
		nodes:     nodeSet,
		flattenDB: leveldb,
	}
}

func (p *PbssStateTrie) Commit() (common.Hash, error) {
	root, nodes, err := p.trie.Commit(true)
	if err != nil {
		return types.EmptyRootHash, err
	}

	if nodes != nil {
		if err := p.nodes.Merge(nodes); err != nil {
			return types.EmptyRootHash, err
		}
	}
	p.db.Update(root, types.EmptyRootHash, 0, p.nodes, nil)
	p.trie, _ = bsctrie.NewStateTrie(bsctrie.TrieID(root), p.db)

	return root, nil
}

func (p *PbssStateTrie) Hash() common.Hash {
	return p.trie.Hash()
}

func (p *PbssStateTrie) Put(key []byte, value []byte) error {
	p.trie.MustUpdate(key, value)
	return nil
}

func (p *PbssStateTrie) Get(key []byte) ([]byte, error) {
	value := p.trie.MustGet(key)
	return value, nil
}

func (p *PbssStateTrie) Delete(key []byte) error {
	p.trie.MustDelete(key)
	return nil
}

func (p *PbssStateTrie) GetMPTEngine() string {
	return StateTrieEngine
}

func (p *PbssStateTrie) GetFlattenDB() ethdb.KeyValueStore {
	return p.flattenDB
}
