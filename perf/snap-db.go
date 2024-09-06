package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"

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
	diskdb                ethdb.KeyValueStore
	triediskdb            ethdb.Database
	triedb                *triedb.Database
	accTrie               *trie.StateTrie
	nodes                 *trienode.MergedNodeSet
	stateTrie             PbssStateTrie
	parentRoot            common.Hash
	height                int64
	accountsOrigin        map[common.Address][]byte                 // The original value of mutated accounts in 'slim RLP' encoding
	storagesOrigin        map[common.Address]map[common.Hash][]byte // The original value of mutated slots in prefix-zero trimmed rlp forma
	stateRoot             common.Hash
	ownerStorageCache     map[common.Hash]common.Hash
	lock                  sync.RWMutex
	trieCacheLock         sync.RWMutex
	ownerStorageTrieCache map[common.Hash]*trie.StateTrie
}

func NewStateRunner(datadir string, root common.Hash) *StateDBRunner {
	triedb, triediskdb, err := MakePBSSTrieDatabase(datadir)

	leveldb, err := rawdb.NewLevelDBDatabase("leveldb", 1000, 20000, "",
		false)
	if err != nil {
		panic("create leveldb err" + err.Error())
	}

	_, diskRoot := rawdb.ReadAccountTrieNode(triediskdb, nil)
	diskRoot = ethTypes.TrieRootHash(diskRoot)
	fmt.Println("disk root is:", diskRoot)

	accTrie, err := trie.NewStateTrie(trie.StateTrieID(diskRoot), triedb)
	if err != nil {
		panic("create state trie err" + err.Error())
	}

	nodeSet := trienode.NewMergedNodeSet()
	if nodeSet == nil {
		panic("node set empty")
	}

	s := &StateDBRunner{
		diskdb:                leveldb,
		triedb:                triedb,
		accTrie:               accTrie,
		nodes:                 nodeSet,
		height:                0,
		parentRoot:            diskRoot,
		accountsOrigin:        make(map[common.Address][]byte),
		storagesOrigin:        make(map[common.Address]map[common.Hash][]byte),
		ownerStorageCache:     make(map[common.Hash]common.Hash),
		ownerStorageTrieCache: make(map[common.Hash]*trie.StateTrie),
		triediskdb:            triediskdb,
		stateRoot:             diskRoot,
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
	return rawdb.ReadAccountSnapshot(v.diskdb, common.BytesToHash([]byte(acckey))), nil
}

func (v *StateDBRunner) GetAccountFromTrie(acckey string) ([]byte, error) {
	return v.accTrie.MustGet([]byte(acckey)), nil
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
	stRoot, err := v.makeStorageTrie(common.BytesToHash(owner), keys, vals)
	if err != nil {
		return err
	}

	acc := &ethTypes.StateAccount{Nonce: uint64(2), Balance: uint256.NewInt(3),
		Root: stRoot, CodeHash: generateCodeHash(owner).Bytes()}

	val, _ := rlp.EncodeToBytes(acc)
	v.AddAccount(string(owner), val)
	return nil
}

func (v *StateDBRunner) makeStorageTrie(owner common.Hash, keys []string, vals []string) (common.Hash, error) {
	id := trie.StorageTrieID(v.stateRoot, owner, ethTypes.EmptyRootHash)
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
	v.lock.Lock()
	v.ownerStorageCache[owner] = root
	v.lock.Unlock()

	return root, nil
}

func (s *StateDBRunner) GetStorage(owner []byte, key []byte) ([]byte, error) {
	return rawdb.ReadStorageSnapshot(s.diskdb, common.BytesToHash(owner), hashData(key)), nil
}

// UpdateStorage  update batch k,v of storage trie
func (s *StateDBRunner) UpdateStorage(owner []byte, keys []string, vals []string) (common.Hash, error) {
	var err error
	ownerHash := common.BytesToHash(owner)
	// try to get version and root from cache first
	var stTrie *trie.StateTrie
	s.trieCacheLock.RLock()
	stTrie, found := s.ownerStorageTrieCache[ownerHash]
	s.trieCacheLock.RUnlock()
	if !found {
		s.lock.RLock()
		root, exist := s.ownerStorageCache[ownerHash]
		s.lock.RUnlock()
		if !exist {
			encodedData, err := s.GetAccount(string(owner))
			if err != nil {
				return ethTypes.EmptyRootHash, fmt.Errorf("fail to get storage trie root in cache1")
			}
			account := new(ethTypes.StateAccount)
			err = rlp.DecodeBytes(encodedData, account)
			if err != nil {
				fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
					err, common.BytesToHash(owner).String(), len(encodedData))
				return ethTypes.EmptyRootHash, err
			}
			root = account.Root
			fmt.Println("new state trie use CA root", root)
		}

		id := trie.StorageTrieID(s.stateRoot, ownerHash, root)
		stTrie, err = trie.NewStateTrie(id, s.triedb)
		if err != nil {
			panic("err new state trie" + err.Error())
		}

		s.trieCacheLock.Lock()
		s.ownerStorageTrieCache[ownerHash] = stTrie
		s.trieCacheLock.Unlock()
	}

	// update batch storage trie
	for i := 0; i < len(keys) && i < len(vals); i++ {
		stTrie.MustUpdate([]byte(keys[i]), []byte(vals[i]))
	}

	root, nodes, err := stTrie.Commit(true)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	if nodes != nil {
		s.nodes.Merge(nodes)
	}

	s.lock.Lock()
	s.ownerStorageCache[ownerHash] = root
	s.lock.Unlock()
	// update the CA account on root tree
	acc := &ethTypes.StateAccount{Nonce: uint64(2), Balance: uint256.NewInt(3),
		Root: root, CodeHash: generateCodeHash(owner).Bytes()}
	val, err := rlp.EncodeToBytes(acc)
	if err != nil {
		panic("encode CA account err" + err.Error())
	}
	accErr := s.UpdateAccount(owner, val)
	if accErr != nil {
		panic("add count err" + accErr.Error())
	}
	s.AddSnapAccount(string(owner), val)

	return root, err
}

func (s *StateDBRunner) RepairSnap(owners []string, largeTrieNum int) {
	for i := 0; i < largeTrieNum; i++ {
		encodedData, err := s.GetAccountFromTrie(owners[i])
		if err != nil {
			panic("fail to repair snap" + err.Error())
		}

		account := new(ethTypes.StateAccount)
		err = rlp.DecodeBytes(encodedData, account)
		if err != nil {
			fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
				err, common.BytesToHash([]byte(owners[i])), len(encodedData))
		}
		root := account.Root
		fmt.Printf("repair the snap of owner hash:%s, repair root %v \n",
			common.BytesToHash([]byte(owners[i])), root)

		s.AddSnapAccount(owners[i], encodedData)
	}

	for i := MaxLargeStorageTrieNum - 1; i < len(owners); i++ {
		encodedData, err := s.GetAccountFromTrie(owners[i])
		if err != nil {
			panic("fail to repair snap" + err.Error())
		}

		account := new(ethTypes.StateAccount)
		err = rlp.DecodeBytes(encodedData, account)
		if err != nil {
			fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
				err, common.BytesToHash([]byte(owners[i])), len(encodedData))
		}
		root := account.Root
		fmt.Printf("repair the snap of owner hash:%s, repair root %v \n",
			common.BytesToHash([]byte(owners[i])), root)

		s.AddSnapAccount(owners[i], encodedData)
	}
}

func (s *StateDBRunner) UpdateAccount(key, value []byte) error {
	originValue, err := s.GetAccount(string(key))
	if err != nil {
		return err
	}
	if bytes.Equal(originValue, value) {
		return nil
	}
	s.accTrie.MustUpdate(key, value)
	return nil
}

func (s *StateDBRunner) InitStorage(owners []common.Hash, trieNum int) {
	return
}

func (s *StateDBRunner) Commit() (common.Hash, error) {
	root, nodes, err := s.accTrie.Commit(false)
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

	s.accTrie, err = trie.NewStateTrie(trie.TrieID(root), s.triedb)
	if err != nil {
		fmt.Println("new acc trie err", err.Error())
	}
	s.parentRoot = root
	s.stateRoot = root
	s.height++
	s.nodes = trienode.NewMergedNodeSet()
	s.trieCacheLock.Lock()
	s.ownerStorageTrieCache = make(map[common.Hash]*trie.StateTrie)
	s.trieCacheLock.Unlock()
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

func (s *StateDBRunner) GetVersion() int64 {
	return -1
}
