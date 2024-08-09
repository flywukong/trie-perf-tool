package main

import (
	"fmt"
	"math/rand"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"
)

type StateDBRunner struct {
	diskdb         ethdb.KeyValueStore
	triedb         *triedb.Database
	accTrie        *trie.StateTrie
	nodes          *trienode.MergedNodeSet
	stateTrie      PbssStateTrie
	parentRoot     common.Hash
	height         int64
	accountsOrigin map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	storagesOrigin map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp forma
}

func NewStateRunner(datadir string, root common.Hash) *StateDBRunner {
	triedb, _ := MakePBSSTrieDatabase(datadir)

	accTrie, err := trie.NewStateTrie(trie.StateTrieID(root), triedb)
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

	s := &StateDBRunner{
		diskdb:         leveldb,
		triedb:         triedb,
		accTrie:        accTrie,
		nodes:          nodeSet,
		height:         0,
		parentRoot:     ethTypes.EmptyRootHash,
		accountsOrigin: make(map[common.Address][]byte),
		storagesOrigin: make(map[common.Address]map[common.Hash][]byte),
	}

	// Initialize with 2 random elements
	s.initializeAccountsOrigin(1)
	s.initializeStoragesOrigin(1)

	return s
}

// Initialize accountsOrigin with n random elements
func (v *StateDBRunner) initializeAccountsOrigin(n int) {
	for i := 0; i < n; i++ {
		addr := common.BytesToAddress(randBytes(20))
		data := randBytes(32)
		v.accountsOrigin[addr] = data
	}
}

// Initialize storagesOrigin with n random elements
func (v *StateDBRunner) initializeStoragesOrigin(n int) {
	for i := 0; i < n; i++ {
		addr := common.BytesToAddress(randBytes(20))
		v.storagesOrigin[addr] = make(map[common.Hash][]byte)
		for j := 0; j < 2; j++ { // Assume each account has 2 storage items
			key := common.BytesToHash(randBytes(32))
			val := randBytes(6)
			v.storagesOrigin[addr][key] = val
		}
	}
}

// Helper function to generate random bytes
func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func (v *StateDBRunner) AddAccount(acckey string, val []byte) error {
	v.accTrie.MustUpdate([]byte(acckey), val)
	return nil
}

func (v *StateDBRunner) GetAccount(acckey string) ([]byte, error) {
	//	key := hashData([]byte(acckey))
	return rawdb.ReadAccountSnapshot(v.diskdb, common.BytesToHash([]byte(acckey))), nil
}

func (v *StateDBRunner) AddSnapAccount(acckey string, val []byte) {
	key := common.BytesToHash([]byte(acckey))
	rawdb.WriteAccountSnapshot(v.diskdb, key, val)
}

func hashData(input []byte) common.Hash {
	var hasher = sha3.NewLegacyKeccak256()
	var hash common.Hash
	hasher.Reset()
	hasher.Write(input)
	hasher.Sum(hash[:0])
	return hash
}

func (v *StateDBRunner) AddStorage(owner []byte, keys []string, vals []string) error {
	stRoot, err := v.makeStorageTrie(hashData(owner), keys, vals)
	if err != nil {
		return err
	}
	acc := &ethTypes.StateAccount{Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: ethTypes.EmptyCodeHash.Bytes()}
	val, _ := rlp.EncodeToBytes(acc)
	v.AddAccount(string(owner), val)
	return nil
}

func (v *StateDBRunner) makeStorageTrie(owner common.Hash, keys []string, vals []string) (common.Hash, error) {
	id := trie.StorageTrieID(ethTypes.EmptyRootHash, owner, ethTypes.EmptyRootHash)
	stTrie, _ := trie.NewStateTrie(id, v.triedb)
	for i, k := range keys {
		stTrie.MustUpdate([]byte(k), []byte(vals[i]))
	}

	root, nodes, err := stTrie.Commit(true)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	if nodes != nil {
		v.nodes.Merge(nodes)
	}
	return root, nil
}

func (s *StateDBRunner) GetStorage(owner []byte, key []byte) ([]byte, error) {
	return rawdb.ReadStorageSnapshot(s.diskdb, common.BytesToHash(owner), hashData(key)), nil
}

/*
func (s *StateDBRunner) UpdateStorage(owner []byte, key []byte, val []byte) ([]byte, error) {

}

*/

func (s *StateDBRunner) Commit() (common.Hash, error) {
	root, nodes, err := s.accTrie.Commit(true)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	if nodes != nil {
		if err := s.nodes.Merge(nodes); err != nil {
			return ethTypes.EmptyRootHash, err
		}
	}

	set := triestate.New(s.accountsOrigin, s.storagesOrigin, nil)
	err = s.triedb.Update(root, s.parentRoot, uint64(s.height), s.nodes, set)
	if err != nil {
		fmt.Println("trie update err", err.Error())
	}

	s.accTrie, _ = trie.NewStateTrie(trie.TrieID(root), s.triedb)
	s.parentRoot = root
	s.height++
	s.nodes = trienode.NewMergedNodeSet()
	return root, nil
}

func (s *StateDBRunner) Hash() common.Hash {
	return s.accTrie.Hash()
}

func (s *StateDBRunner) GetMPTEngine() string {
	return StateTrieEngine
}

func (p *StateDBRunner) GetFlattenDB() ethdb.KeyValueStore {
	return p.diskdb
}
