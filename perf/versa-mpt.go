package main

import (
	"github.com/bnb-chain/versioned-state-database/store"

	versa_tree "github.com/bnb-chain/versioned-state-database/tree"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
)

type VersaTrie struct {
	trie versa_tree.Tree
}

func OpenVersaTrie(version int64, rootHash []byte) *VersaTrie {
	store, _, _, err := store.Open("test-versa-trie", nil)
	if err != nil {
		panic(err.Error())
	}
	t := versa_tree.OpenTree(common.Hash{}, version, rootHash, false, store)
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
