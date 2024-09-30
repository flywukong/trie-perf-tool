package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/ethereum/go-ethereum/triedb"
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
	updatelock            sync.Mutex
	ownerStorageTrieCache map[common.Hash]*trie.StateTrie
	cache                 *fastcache.Cache
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
		cache:                 fastcache.New(1024 * 1024 * 1024),
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

type hasher struct {
	sha      crypto.KeccakState
	tmp      []byte
	encbuf   rlp.EncoderBuffer
	parallel bool // Whether to use parallel threads when hashing
}

// hasherPool holds pureHashers
var hasherPool = sync.Pool{
	New: func() interface{} {
		return &hasher{
			tmp:    make([]byte, 0, 550), // cap is as large as a full fullNode.
			sha:    sha3.NewLegacyKeccak256().(crypto.KeccakState),
			encbuf: rlp.NewEncoderBuffer(nil),
		}
	},
}

func newHasher(parallel bool) *hasher {
	h := hasherPool.Get().(*hasher)
	h.parallel = parallel
	return h
}

func returnHasherToPool(h *hasher) {
	hasherPool.Put(h)
}

// no use hashKeyBuf for thread safe.
func (t *StateDBRunner) hashKey(key []byte) []byte {
	hash := make([]byte, common.HashLength)
	h := newHasher(false)
	h.sha.Reset()
	h.sha.Write(key)
	h.sha.Read(hash)
	returnHasherToPool(h)
	return hash
}

func (s *StateDBRunner) AddAccount(address common.Address, account *ethTypes.StateAccount) error {
	return s.accTrie.UpdateAccount(address, account)
}

func (s *StateDBRunner) GetAccount(address common.Address) ([]byte, error) {
	accHash := s.hashKey(address.Bytes())
	if blob, found := s.cache.HasGet(nil, accHash[:]); found {
		snapshotCleanAccountHitMeter.Mark(1)
		return blob, nil
	}
	snapshotCleanAccountMissMeter.Mark(1)
	data := rawdb.ReadAccountSnapshot(s.diskdb, common.BytesToHash(accHash))
	//	rawdb.WriteAccountSnapshot(snapDB, accHash, data)
	s.cache.Set(accHash[:], data)
	return data, nil
}

func (v *StateDBRunner) GetAccountFromTrie(address common.Address) ([]byte, error) {
	return v.accTrie.MustGet(address.Bytes()), nil
}

func (s *StateDBRunner) AddSnapAccount(address common.Address, acc *ethTypes.StateAccount) error {
	accHash := s.hashKey(address.Bytes())
	data, err := rlp.EncodeToBytes(acc)
	if err != nil {
		return err
	}
	rawdb.WriteAccountSnapshot(s.diskdb, common.BytesToHash(accHash), data)
	return nil
}

func hashData(input []byte) common.Hash {
	var hasher = sha3.NewLegacyKeccak256()
	var hash common.Hash
	hasher.Reset()
	hasher.Write(input)
	hasher.Sum(hash[:0])
	return hash
}

func (v *StateDBRunner) AddStorage(address common.Address, keys []string, vals []string) error {
	stRoot, err := v.makeStorageTrie(address, keys, vals)
	if err != nil {
		return err
	}

	nonce, balance := getRandomBalance()
	acc := &ethTypes.StateAccount{Nonce: nonce, Balance: balance,
		Root: stRoot, CodeHash: generateCodeHash(address.Bytes()).Bytes()}

	v.AddAccount(address, acc)
	return nil
}

func (v *StateDBRunner) makeStorageTrie(address common.Address, keys []string, vals []string) (common.Hash, error) {
	owner := crypto.Keccak256Hash(address.Bytes())
	id := trie.StorageTrieID(v.stateRoot, owner, ethTypes.EmptyRootHash)
	stTrie, _ := trie.NewStateTrie(id, v.triedb)
	for i, k := range keys {
		err := stTrie.UpdateStorage(address, []byte(k), []byte(vals[i]))
		if err != nil {
			return ethTypes.EmptyRootHash, err
		}
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

func (s *StateDBRunner) GetStorage(address common.Address, key []byte) ([]byte, error) {
	accHash := s.hashKey(address.Bytes())
	storageHash := hashData(key)
	cachekey := append(accHash[:], storageHash[:]...)
	// Try to retrieve the storage slot from the memory cache
	if blob, found := s.cache.HasGet(nil, cachekey); found {
		snapshotCleanStorageHitMeter.Mark(1)
		return blob, nil
	}
	snapshotCleanStorageMissMeter.Mark(1)
	data := rawdb.ReadStorageSnapshot(s.diskdb, common.BytesToHash(accHash), storageHash)

	s.cache.Set(cachekey, data)
	return data, nil
}

func (s *StateDBRunner) GetStorageFromTrie(address common.Address, key []byte) ([]byte, error) {
	var err error
	ownerHash := crypto.Keccak256Hash(address.Bytes())
	// try to get version and root from cache first
	var stTrie *trie.StateTrie
	s.trieCacheLock.RLock()
	stTrie, found := s.ownerStorageTrieCache[ownerHash]
	s.trieCacheLock.RUnlock()
	if !found {
		fmt.Println("fail to find the tree handler in cache")
		s.lock.RLock()
		root, exist := s.ownerStorageCache[ownerHash]
		s.lock.RUnlock()
		if !exist {
			encodedData, err := s.GetAccount(address)
			if err != nil {
				return nil, fmt.Errorf("fail to get storage trie root in cache1")
			}
			account := new(ethTypes.StateAccount)
			err = rlp.DecodeBytes(encodedData, account)
			if err != nil {
				fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
					err, ownerHash.String(), len(encodedData))
				return nil, err
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
	return stTrie.GetStorage(address, key)
}

// UpdateStorage  update batch k,v of storage trie
func (s *StateDBRunner) UpdateStorage(address common.Address, keys []string, vals []string) (common.Hash, error) {
	var err error
	ownerHash := crypto.Keccak256Hash(address.Bytes())
	// try to get version and root from cache first
	var stTrie *trie.StateTrie
	s.trieCacheLock.RLock()
	stTrie, found := s.ownerStorageTrieCache[ownerHash]
	s.trieCacheLock.RUnlock()
	if !found {
		fmt.Println("fail to find the tree handler in cache")
		s.lock.RLock()
		root, exist := s.ownerStorageCache[ownerHash]
		s.lock.RUnlock()
		if !exist {
			encodedData, err := s.GetAccount(address)
			if err != nil {
				return ethTypes.EmptyRootHash, fmt.Errorf("fail to get storage trie root in cache1")
			}
			account := new(ethTypes.StateAccount)
			err = rlp.DecodeBytes(encodedData, account)
			if err != nil {
				fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
					err, ownerHash.String(), len(encodedData))
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
		originValue, _ := stTrie.GetStorage(address, []byte(keys[i]))
		if !bytes.Equal(originValue, []byte(vals[i])) {
			fmt.Println("update value not same")
		}

		start := time.Now()
		err = stTrie.UpdateStorage(address, []byte(keys[i]), []byte(vals[i]))
		if err != nil {
			fmt.Println("update storage error", err.Error())
		}
		StateTreeUpdateTime.Update(time.Since(start))

	}

	root, nodes, err := stTrie.Commit(true)
	if err != nil {
		return ethTypes.EmptyRootHash, err
	}
	s.updatelock.Lock()
	if nodes != nil {
		s.nodes.Merge(nodes)
	}

	// update the CA account on root tree
	nonce, balance := getRandomBalance()
	acc := &ethTypes.StateAccount{Nonce: nonce, Balance: balance,
		Root: root, CodeHash: generateCodeHash(address.Bytes()).Bytes()}

	accErr := s.UpdateAccount(address, acc)
	if accErr != nil {
		panic("add count err" + accErr.Error())
	}
	s.updatelock.Unlock()

	s.AddSnapAccount(address, acc)
	s.lock.Lock()
	s.ownerStorageCache[ownerHash] = root
	s.lock.Unlock()

	return root, err
}

func (s *StateDBRunner) OpenStorageTries(addresses []common.Address) error {
	var err error
	for i := 0; i < len(addresses); i++ {
		address := addresses[i]
		ownerHash := crypto.Keccak256Hash(address.Bytes())

		var stTrie *trie.StateTrie
		s.trieCacheLock.RLock()
		stTrie, found := s.ownerStorageTrieCache[ownerHash]
		s.trieCacheLock.RUnlock()
		if !found {
			s.lock.RLock()
			root, exist := s.ownerStorageCache[ownerHash]
			s.lock.RUnlock()
			if !exist {
				encodedData, err := s.GetAccountFromTrie(address)
				if err != nil {
					return fmt.Errorf("fail to get storage trie root in cache1")
				}
				account := new(ethTypes.StateAccount)
				err = rlp.DecodeBytes(encodedData, account)
				if err != nil {
					fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
						err, ownerHash.String(), len(encodedData))
					return err
				}
				root = account.Root
				//	fmt.Println("new state trie use CA root", root)
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
	}
	return nil
}

func (s *StateDBRunner) RepairSnap(addresses []common.Address, largeTrieNum int) {
	for i := 0; i < largeTrieNum; i++ {
		encodedData, err := s.GetAccountFromTrie(addresses[i])
		if err != nil {
			panic("fail to repair snap" + err.Error())
		}

		ownerHash := crypto.Keccak256Hash(addresses[i].Bytes())
		account := new(ethTypes.StateAccount)
		err = rlp.DecodeBytes(encodedData, account)
		if err != nil {
			fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
				err, ownerHash, len(encodedData))
		}
		root := account.Root
		fmt.Printf("repair the snap of owner hash:%s, repair root %v \n",
			ownerHash, root)

		s.AddSnapAccount(addresses[i], account)
	}

	for i := MaxLargeStorageTrieNum - 1; i < len(addresses); i++ {
		encodedData, err := s.GetAccountFromTrie(addresses[i])
		if err != nil {
			panic("fail to repair snap" + err.Error())
		}
		ownerHash := crypto.Keccak256Hash(addresses[i].Bytes())
		account := new(ethTypes.StateAccount)
		err = rlp.DecodeBytes(encodedData, account)
		if err != nil {
			fmt.Printf("failed to decode RLP %v, db get CA account %s, val len:%d \n",
				err, ownerHash, len(encodedData))
		}
		root := account.Root
		fmt.Printf("repair the snap of owner hash:%s, repair root %v \n",
			ownerHash, root)

		s.AddSnapAccount(addresses[i], account)
	}
}

func (s *StateDBRunner) UpdateAccount(address common.Address, acc *ethTypes.StateAccount) error {
	/*
		originValue, err := s.GetAccount(string(key))
		if err != nil {
			return err
		}
		if bytes.Equal(originValue, value) {
			return nil
		}

	*/
	return s.accTrie.UpdateAccount(address, acc)
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

func (s *StateDBRunner) GetCache() *fastcache.Cache {
	return s.cache
}
