package main

import (
	"path/filepath"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
	bsctrie "github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
)

type PbssRawTrie struct {
	trie  *bsctrie.Trie
	db    *triedb.Database
	nodes *trienode.MergedNodeSet
}

func OpenPbssDB(dataDir string, root common.Hash) *PbssRawTrie {
	triedb, _, _ := MakePBSSTrieDatabase(dataDir)

	t, err := bsctrie.New(bsctrie.StateTrieID(root), triedb)
	if err != nil {
		panic("create state trie err")
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}
	return &PbssRawTrie{
		trie:  t,
		db:    triedb,
		nodes: nodeSet,
	}
}

func (p *PbssRawTrie) Commit() (common.Hash, error) {
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
	//	p.db.Commit
	p.trie, _ = bsctrie.New(bsctrie.TrieID(root), p.db)

	return root, nil
}

func (p *PbssRawTrie) Hash() common.Hash {
	return p.trie.Hash()
}

func (p *PbssRawTrie) Put(key []byte, value []byte) error {
	return p.trie.Update(key, value)
}

func (p *PbssRawTrie) Get(key []byte) ([]byte, error) {
	return p.trie.Get(key)
}

func (p *PbssRawTrie) Delete(key []byte) error {
	return p.trie.Delete(key)
}

func (p *PbssRawTrie) GetFlattenDB() ethdb.KeyValueStore {
	return nil
}

func (p *PbssRawTrie) GetMPTEngine() string {
	return PbssRawTrieEngine
}

func createChainDataBase(datadir string) (ethdb.Database, error) {
	db, err := OpenDatabaseWithFreezer("chaindata", 1000, 20000, "", "",
		datadir)
	if err != nil {
		return nil, err
	}
	return db, err
}

// MakePBSSTrieDatabase constructs a trie database based on the configured scheme.
func MakePBSSTrieDatabase(datadir string) (*triedb.Database, ethdb.Database, error) {
	diskdb, err := createChainDataBase(datadir)
	if err != nil {
		return nil, diskdb, err
	}
	config := &triedb.Config{
		Cache: TrieCleanCache,
	}
	config.PathDB = &pathdb.Config{
		StateHistory:   params.FullImmutabilityThreshold,
		CleanCacheSize: TrieCleanCache * 1024 * 1024 * 6,
		DirtyCacheSize: TrieCleanCache * 1024 * 1024,
	}

	//config.PathDB.JournalFilePath = fmt.Sprintf("%s/%s", stack.ResolvePath("chaindata"), eth.JournalFileName)
	return triedb.NewDatabase(diskdb, config), diskdb, nil
}

func OpenDatabaseWithFreezer(name string, cache, handles int, ancient, namespace, datadir string) (ethdb.Database, error) {
	data := filepath.Join(datadir, "geth")
	directory := filepath.Join(data, name)
	db, err := rawdb.Open(rawdb.OpenOptions{
		Type:              "pebble",
		Directory:         directory,
		AncientsDirectory: filepath.Join(directory, "ancient"),
		Namespace:         namespace,
		Cache:             cache,
		Handles:           handles,
		ReadOnly:          false,
	})
	return db, err
}
