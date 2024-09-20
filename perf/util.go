package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type CAKeyValue struct {
	Keys []string
	Vals []string
}

const (
	CAStorageSize          = 100
	CAStorageUpdateNum     = 100
	CAStorageTrieNum       = 10
	CAStorageInitSize      = 10000000
	InitAccounts           = 10000000
	AccountKeyCacheSize    = 200000
	LargeStorageTrieNum    = 2
	MaxLargeStorageTrieNum = 20
	MaxCATrieNum           = 20000
	SmallTriesReadInBlock  = 28
)

type TreeConfig struct {
	LargeTrees []common.Hash `toml:"large_trees"`
	SmallTrees []common.Hash `toml:"small_trees"`
}

func NewConfig(largeTrees []common.Hash, smallTrees []common.Hash) *TreeConfig {
	return &TreeConfig{
		LargeTrees: largeTrees,
		SmallTrees: smallTrees,
	}
}

var InitFinishRoot = []byte("perf-init-root")

type InitDBTask map[string]CAKeyValue

type DBTask struct {
	AccountTask   map[common.Address]*types.StateAccount
	SmallTrieTask map[common.Address]CAKeyValue
	LargeTrieTask map[common.Address]CAKeyValue
}

type VerifyTask struct {
	AccountTask map[string][]byte
	StorageTask map[string]CAKeyValue
}

func NewDBTask() DBTask {
	return DBTask{
		AccountTask:   make(map[common.Address]*types.StateAccount),
		SmallTrieTask: make(map[common.Address]CAKeyValue),
		LargeTrieTask: make(map[common.Address]CAKeyValue),
	}
}

func NewVerifyTask() VerifyTask {
	return VerifyTask{
		AccountTask: make(map[string][]byte),
		StorageTask: make(map[string]CAKeyValue),
	}
}

type Stat struct {
	ioStat      IOStat
	lastIoStat  IOStat
	startIOStat IOStat // Initial IOStat when the service starts
	totalIOStat IOStat // Accumulated IOStat for average calculation
}

type IOStat struct {
	get         uint64
	put         uint64
	getNotExist uint64
	delete      uint64
}

func NewStat() *Stat {
	startIOStat := IOStat{
		get:         0,
		put:         0,
		delete:      0,
		getNotExist: 0,
	}
	return &Stat{
		ioStat:      startIOStat,
		lastIoStat:  startIOStat,
		startIOStat: startIOStat,
		totalIOStat: startIOStat,
	}
}

func (s *Stat) CalcTpsAndOutput(delta time.Duration) string {
	get := atomic.LoadUint64(&s.ioStat.get)
	put := atomic.LoadUint64(&s.ioStat.put)
	del := atomic.LoadUint64(&s.ioStat.delete)
	getNotExist := atomic.LoadUint64(&s.ioStat.getNotExist)

	deltaF64 := delta.Seconds()

	getTps := float64(get-atomic.LoadUint64(&s.lastIoStat.get)) / deltaF64
	putTps := float64(put-atomic.LoadUint64(&s.lastIoStat.put)) / deltaF64
	deleteTps := float64(del-atomic.LoadUint64(&s.lastIoStat.delete)) / deltaF64
	getNotExistTps := float64(getNotExist-atomic.LoadUint64(&s.lastIoStat.getNotExist)) / deltaF64

	// Update total IOStat for average calculation
	s.totalIOStat = IOStat{
		get:         s.totalIOStat.get + (get - atomic.LoadUint64(&s.lastIoStat.get)),
		put:         s.totalIOStat.put + (put - atomic.LoadUint64(&s.lastIoStat.put)),
		delete:      s.totalIOStat.delete + (del - atomic.LoadUint64(&s.lastIoStat.delete)),
		getNotExist: s.totalIOStat.getNotExist + (getNotExist - atomic.LoadUint64(&s.lastIoStat.getNotExist)),
	}

	// keep io stat snapshot
	atomic.StoreUint64(&s.lastIoStat.get, get)
	atomic.StoreUint64(&s.lastIoStat.put, put)
	atomic.StoreUint64(&s.lastIoStat.delete, del)
	atomic.StoreUint64(&s.lastIoStat.getNotExist, getNotExist)

	return fmt.Sprintf(
		"tps: [get=%.2f, put=%.2f, delete=%.2f, get_not_exist=%.2f]",
		getTps, putTps, deleteTps, getNotExistTps,
	)
}

// CalcAverageIOStat calculates the average IOStat and returns a formatted string.
func (s *Stat) CalcAverageIOStat(duration time.Duration) string {
	durationF64 := duration.Seconds()

	avgGet := float64(s.totalIOStat.get) / durationF64
	avgPut := float64(s.totalIOStat.put) / durationF64
	avgDelete := float64(s.totalIOStat.delete) / durationF64
	avgGetNotExist := float64(s.totalIOStat.getNotExist) / durationF64

	return fmt.Sprintf(
		"average tps: [get=%.2f, put=%.2f, delete=%.2f, get_not_exist=%.2f]",
		avgGet, avgPut, avgDelete, avgGetNotExist,
	)
}

func (s *Stat) IncPut(num uint64) {
	atomic.AddUint64(&s.ioStat.put, num)
}

func (s *Stat) IncGet(num uint64) {
	atomic.AddUint64(&s.ioStat.get, num)
}

func (s *Stat) IncGetNotExist(num uint64) {
	atomic.AddUint64(&s.ioStat.getNotExist, num)
}

func (s *Stat) IncDelete(num uint64) {
	atomic.AddUint64(&s.ioStat.delete, num)
}

func makeAccounts(size int) (addresses [][20]byte, accounts [][]byte) {
	random := rand.New(rand.NewSource(0))
	// Create a realistic account trie to hash
	addresses = make([][20]byte, size)

	for i := 0; i < len(addresses); i++ {
		data := make([]byte, 20)
		random.Read(data)
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
		copy(addresses[i][:], data)
	}

	accounts = make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce = uint64(random.Int63())
			root  = types.EmptyRootHash
			code  = crypto.Keccak256(nil)
		)
		numBytes := random.Uint32() % 33 // [0, 32] bytes
		balanceBytes := make([]byte, numBytes)
		random.Read(balanceBytes)
		balance := new(uint256.Int).SetBytes(balanceBytes)
		data, _ := rlp.EncodeToBytes(&types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code})
		accounts[i] = data
	}
	return addresses, accounts
}

func makeAccountsV3(size int) (addresses [][20]byte, accounts []*types.StateAccount) {
	random := rand.New(rand.NewSource(0))
	// Create a realistic account trie to hash
	addresses = make([][20]byte, size)

	for i := 0; i < len(addresses); i++ {
		data := make([]byte, 20)
		random.Read(data)
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(data), func(i, j int) { data[i], data[j] = data[j], data[i] })
		copy(addresses[i][:], data)
	}

	accounts = make([]*types.StateAccount, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce = uint64(random.Int63())
			root  = types.EmptyRootHash
			code  = crypto.Keccak256(nil)
		)
		numBytes := random.Uint32() % 33 // [0, 32] bytes
		balanceBytes := make([]byte, numBytes)
		random.Read(balanceBytes)
		balance := new(uint256.Int).SetBytes(balanceBytes)
		accounts[i] = &types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code}
	}
	return addresses, accounts
}

func makeAccountsV2(startIndex, size uint64) (addresses [][20]byte, accounts []*types.StateAccount) {
	random := rand.New(rand.NewSource(0))
	// Create a realistic account trie to hash
	addresses = make([][20]byte, size)

	for i := uint64(0); i < size; i++ {
		num := startIndex + i + MaxCATrieNum
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		//	fmt.Println("init account use num", num)
		copy(addresses[i][:], hash[:20])
	}

	accounts = make([]*types.StateAccount, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce = uint64(random.Int63())
			root  = types.EmptyRootHash
			code  = crypto.Keccak256(nil)
		)
		numBytes := random.Uint32() % 33 // [0, 32] bytes
		balanceBytes := make([]byte, numBytes)
		random.Read(balanceBytes)
		balance := new(uint256.Int).SetBytes(balanceBytes)
		accounts[i] = &types.StateAccount{Nonce: nonce, Balance: balance, Root: root, CodeHash: code}
	}
	return addresses, accounts
}

func getRandomBalance() (uint64, *uint256.Int) {
	random := rand.New(rand.NewSource(0))
	numBytes := random.Uint32() % 33 // [0, 32] bytes
	balanceBytes := make([]byte, numBytes)
	random.Read(balanceBytes)
	balance := new(uint256.Int).SetBytes(balanceBytes)
	return uint64(random.Int63()), balance
}

func genAccountKey(totalSize, size uint64) []string {
	// Create a realistic account trie to hash
	addressList := make([]string, size)
	addresses := make([][20]byte, size)
	for i := uint64(0); i < size; i++ {
		num := rand.Intn(int(totalSize)) + MaxCATrieNum
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		copy(addresses[i][:], hash[:20])
	}
	for i := 0; i < len(addresses); i++ {
		initKey := string(crypto.Keccak256(addresses[i][:]))
		addressList[i] = initKey
	}
	return addressList
}

func genAccountKeyV2(totalSize, size uint64) [][20]byte {
	// Create a realistic account trie to hash
	addresses := make([][20]byte, size)
	for i := uint64(0); i < size; i++ {
		num := rand.Intn(int(totalSize)) + MaxCATrieNum
		//fmt.Println("account generate num", num)
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		//	fmt.Println("account generate hash", common.BytesToHash(hash))
		copy(addresses[i][:], hash[:20])
	}

	return addresses
}

func genOwnerHashKey(size int) (addresses []string) {
	// Create a realistic account trie to hash
	addresses = make([]string, size)

	for i := 1; i < size+1; i++ {
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", i)))
		addresses[i-1] = string(hash)
		fmt.Println("generate tree owner hash", common.BytesToHash([]byte(addresses[i-1])))
	}
	return addresses
}

func genOwnerHashKeyV2(size int) (addresses [][]byte) {
	// Create a realistic account trie to hash
	addresses = make([][]byte, size)

	for i := 1; i < size+1; i++ {
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", i)))
		addresses[i-1] = hash[:20]
		fmt.Println("generate tree owner hash", crypto.Keccak256Hash(addresses[i-1]))
	}
	return addresses
}

func genStorageTrieKeyV1(startIndex, size uint64) (addresses []string) {
	// Create a realistic account trie to hash
	addresses = make([]string, size)

	for i := uint64(0); i < size; i++ {
		num := startIndex + i
		hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		addresses[i] = string(hash[:20])
	}
	return addresses
}

func genStorageTrieKey(ownerHash common.Hash, startIndex, size uint64) (addresses []string) {
	// Create a realistic account trie to hash
	addresses = make([]string, size)

	for i := uint64(0); i < size; i++ {
		num := startIndex + i
		//	hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		/*
			numbytes := fmt.Sprintf("%d", num)
			numLen := len(ownerHash) - len(numbytes)
			addresses[i] = string(crypto.Keccak256Hash([]byte(ownerHash.String() + numbytes)))
		*/
		numbytes := []byte(fmt.Sprintf("%d", num))

		key := append(ownerHash[:], numbytes[:]...)
		addresses[i] = string(crypto.Keccak256(key))
		/*
			if i > 0 && i%100 == 0 {
				fmt.Println("numbyetes", string(numbytes), "owner hash", ownerHash.String(), "key", string(key),
					"trie key len", len([]byte(addresses[i])))
			}

		*/

	}
	return addresses
}

func genStorageTrieKeyV3(ownerHash common.Hash, startIndex, size uint64) (addresses []string) {
	// Create a realistic account trie to hash
	addresses = make([]string, size)

	for i := uint64(0); i < size; i++ {
		num := startIndex + i
		//	hash := crypto.Keccak256([]byte(fmt.Sprintf("%d", num)))
		numbytes := fmt.Sprintf("%d", num)
		numLen := len(ownerHash) - len(numbytes)
		addresses[i] = string(crypto.Keccak256([]byte(ownerHash.String()[:numLen] + numbytes)))
		/*
			if i == uint64(0) {
				fmt.Println("storage key size:", len(addresses[i]))
			}
		*/
	}
	return addresses
}

// randomFloat returns a random float64 between 0 and 1
func randomFloat() float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64()
}

func generateValue(minSize, maxSize uint64) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	size := minSize + uint64(rand.Intn(int(maxSize-minSize+1)))
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func generateValueV2(minSize, maxSize uint64) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	// We want the average length to be around 13.5, so we need to adjust the size generation.
	// Using a custom weighted distribution to achieve this:
	size := customRandomSize(minSize, maxSize)
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func customRandomSize(minSize, maxSize uint64) uint64 {
	// Example of using triangular distribution centered around 13.5
	avgSize := 13.5
	randomFactor := rand.Float64()

	// Use a weighted average with bias towards the center
	if randomFactor < 0.5 {
		// Generate values between minSize and avgSize (bias towards the middle)
		return minSize + uint64(rand.Float64()*(avgSize-float64(minSize)))
	} else {
		// Generate values between avgSize and maxSize (bias towards the middle)
		return uint64(avgSize) + uint64(rand.Float64()*(float64(maxSize)-avgSize))
	}
}

func generateKey(size int64) []byte {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

type InsertedKeySet struct {
	mu      sync.RWMutex
	items   []string
	maxSize int
	index   int
}

func NewFixedSizeSet(maxSize int) *InsertedKeySet {
	return &InsertedKeySet{
		items:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (s *InsertedKeySet) Add(item string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.items) < s.maxSize {
		s.items = append(s.items, item)
	} else {
		s.items[s.index] = item
		s.index = (s.index + 1) % s.maxSize
	}
}

func (s *InsertedKeySet) RandomItem() (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.items) == 0 {
		return "", false
	}
	randomIndex := rand.Intn(len(s.items))
	return s.items[randomIndex], true
}

func (s *InsertedKeySet) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Reset items slice and other relevant state
	s.items = make([]string, 0, s.maxSize)
	s.index = 0
}

func generateRandomBytes(length int) []byte {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return nil
	}
	return bytes
}

func splitAccountTask(originalMap map[common.Address]*types.StateAccount, n int) []map[common.Address]*types.StateAccount {
	if n <= 0 {
		return nil
	}

	keys := make([]common.Address, 0, len(originalMap))
	for k := range originalMap {
		keys = append(keys, k)
	}

	partitionSize := int(math.Ceil(float64(len(keys)) / float64(n)))

	partitions := make([]map[common.Address]*types.StateAccount, 0, n)
	for i := 0; i < n; i++ {
		partitions = append(partitions, make(map[common.Address]*types.StateAccount))
	}

	for i, key := range keys {
		part := i / partitionSize
		if part >= n {
			part = n - 1
		}
		partitions[part][key] = originalMap[key]
	}

	return partitions
}

func splitTrieTask(originalMap map[common.Address]CAKeyValue, n int) []map[common.Address]CAKeyValue {
	if n <= 0 {
		return nil
	}

	keys := make([]common.Address, 0, len(originalMap))
	for k := range originalMap {
		keys = append(keys, k)
	}

	partitionSize := int(math.Ceil(float64(len(keys)) / float64(n)))

	partitions := make([]map[common.Address]CAKeyValue, n)
	for i := range partitions {
		partitions[i] = make(map[common.Address]CAKeyValue)
	}

	for i, key := range keys {
		part := i / partitionSize
		if part >= n {
			part = n - 1
		}
		partitions[part][key] = originalMap[key]
	}

	return partitions
}

func generateCodeHash(owner []byte) common.Hash {
	data := append(owner, []byte("code")...)
	return crypto.Keccak256Hash(data)
}
