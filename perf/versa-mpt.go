package main

import (
	versaTree "github.com/bnb-chain/versioned-state-database/tree"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

type VersaTrie struct {
	trie versaTree.Tree
}

func OpenVersaTrie(version int64, rootHash []byte) *VersaTrie {
	/*
		store, _, _, err := store.Open("test-versa-trie", &store.Options{
			//	PageDecoder: &versaTree.PageDecoder{},
		})
		if err != nil {
			panic(err.Error())
		}

	*/
	t := versaTree.OpenTree(common.Hash{}, version, rootHash, false, nil)
	return &VersaTrie{
		trie: t,
	}
}

func (p *VersaTrie) Hash() common.Hash {
	return p.trie.HashRoot()
}

func (p *VersaTrie) Put(key []byte, value []byte) error {
	return p.trie.Insert(key, value)
}

func (p *VersaTrie) Get(key []byte) ([]byte, error) {
	_, value, err := p.trie.Get(key)
	return value, err
}

func (p *VersaTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func (p *VersaTrie) Commit() (common.Hash, error) {
	hash, _, err := p.trie.Commit(0)
	return hash, err
}

func (p *VersaTrie) GetMPTEngine() string {
	return VERSADBEngine
}

func (p *VersaTrie) GetFlattenDB() ethdb.KeyValueStore {
	return nil
}
