package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/holiman/uint256"
)

type CAKeyValue struct {
	Keys    []string
	Vals    []string
	Account []byte
}

const (
	CAStorageSize      = 100
	CAStorageUpdateNum = 100
	CAStorageTrieNum   = 10
	CAStorageInitSize  = 10000000
)

type DBTask map[string]CAKeyValue

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

func makeStorage(size int) (addresses [][20]byte, accounts [][]byte) {
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

type InsertedKeySet struct {
	mu      sync.Mutex
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
	//s.mu.Lock()
	//defer s.mu.Unlock()
	if len(s.items) == 0 {
		return "", false
	}
	randomIndex := rand.Intn(len(s.items))
	return s.items[randomIndex], true
}

// GetNRandomSets returns n slices, each containing m unique keys. No key is repeated across slices.
func (s *InsertedKeySet) GetNRandomSets(n int, m int) ([][]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	totalItems := len(s.items)
	if n*m > totalItems {
		return nil, fmt.Errorf("not enough items in the set to generate %d sets of %d items", n, m)
	}

	result := make([][]string, n)
	allIndices := rand.Perm(totalItems) // Generate a random permutation of indices
	for i := 0; i < n; i++ {
		set := make([]string, m)
		for j := 0; j < m; j++ {
			set[j] = s.items[allIndices[i*m+j]]
		}
		result[i] = set
	}

	return result, nil
}
