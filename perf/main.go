package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/metrics/exp"
	"github.com/urfave/cli/v2"
)

const (
	PbssRawTrieEngine = "pbss-mpt"
	VERSADBEngine     = "versa-mpt"
	StateTrieEngine   = "secure-trie"
)

// PerfConfig struct to hold command line arguments
type PerfConfig struct {
	Engine           string
	DataDir          string
	BatchSize        uint64
	NumJobs          int
	KeyRange         uint64
	MinValueSize     uint64
	MaxValueSize     uint64
	DeleteRatio      float64
	RwRatio          float64
	MetricsAddr      string
	MetricsPort      int
	StorageTrieSize  uint64
	SmallStorageSize uint64
	StorageTrieNum   uint64
	LargeTrieNum     uint64
	AccountsInitSize uint64
	AccountsBlocks   uint64
	TrieBlocks       uint64
	IsInitMode       bool
	SleepTime        uint64
}

const version = "1.0.0"

// Run is the function to runPerf the bsperftool command line tool
func main() {
	var config PerfConfig

	app := &cli.App{
		Name:    "perftool",
		Usage:   "A tool to perform state db benchmarking",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "press-test",
				Usage: "Press random keys into the trie database",
				Action: func(c *cli.Context) error {
					runPerf(c)
					return nil
				},
			},
			{
				Name:  "press-db",
				Usage: "Press random keys into the trie database",
				Action: func(c *cli.Context) error {
					runPerfDB(c)
					return nil
				},
			},
			{
				Name:  "verify-hash",
				Usage: "verify hash root of trie database by comparing",
				Action: func(c *cli.Context) error {
					verifyHash(c)
					return nil
				},
			},
			{
				Name:  "verify-db",
				Usage: "verify hash root of state database by comparing",
				Action: func(c *cli.Context) error {
					verifyDB(c)
					return nil
				},
			},
		},

		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "engine",
				Aliases:     []string{"e"},
				Usage:       "Engine to use, engine can be pbss-mpt,versa-mpt or secure-trie",
				Destination: &config.Engine,
			},
			&cli.StringFlag{
				Name:        "datadir",
				Aliases:     []string{"d"},
				Usage:       "Data directory",
				Value:       "./dataset",
				Destination: &config.DataDir,
			},
			&cli.StringFlag{
				Name:        "metrics.addr",
				Aliases:     []string{"ma"},
				Usage:       "metrics address",
				Value:       "127.0.0.1",
				Destination: &config.MetricsAddr,
			},
			&cli.IntFlag{
				Name:        "metrics.port",
				Aliases:     []string{"mp"},
				Usage:       "Metrics HTTP server listening port",
				Value:       8545,
				Destination: &config.MetricsPort,
			},
			&cli.BoolFlag{
				Name:  "metrics",
				Usage: "Enable metrics collection and reporting",
			},

			&cli.Uint64Flag{
				Name:        "bs",
				Aliases:     []string{"b"},
				Usage:       "Batch size",
				Value:       1000,
				Destination: &config.BatchSize,
			},
			&cli.IntFlag{
				Name:        "threads",
				Aliases:     []string{"t"},
				Usage:       "Number of threads",
				Value:       10,
				Destination: &config.NumJobs,
			},
			&cli.Uint64Flag{
				Name:        "triesize",
				Aliases:     []string{"ts"},
				Usage:       "storage trie size",
				Value:       10000000,
				Destination: &config.StorageTrieSize,
			},
			&cli.Uint64Flag{
				Name:        "sleeptime",
				Aliases:     []string{"stime"},
				Usage:       "sleep time of block",
				Value:       500,
				Destination: &config.SleepTime,
			},
			&cli.Uint64Flag{
				Name:        "smallstorage",
				Aliases:     []string{"ss"},
				Usage:       "small storage trie size",
				Value:       150000,
				Destination: &config.SmallStorageSize,
			},
			&cli.Uint64Flag{
				Name:        "trienum",
				Aliases:     []string{"tn"},
				Usage:       "storage trie num",
				Value:       10,
				Destination: &config.StorageTrieNum,
			},
			&cli.Uint64Flag{
				Name:        "largetries",
				Aliases:     []string{"lt"},
				Usage:       "large storage trie num",
				Value:       3,
				Destination: &config.LargeTrieNum,
			},

			&cli.Uint64Flag{
				Name:        "accounts",
				Aliases:     []string{"a"},
				Usage:       "account size",
				Value:       1000000,
				Destination: &config.AccountsInitSize,
			},
			&cli.Uint64Flag{
				Name:        "account_block",
				Aliases:     []string{"ab"},
				Usage:       "blocks number to init the account",
				Value:       1000,
				Destination: &config.AccountsBlocks,
			},
			&cli.Uint64Flag{
				Name:        "trie_block",
				Aliases:     []string{"tb"},
				Usage:       "blocks to init the large tree",
				Value:       1000,
				Destination: &config.TrieBlocks,
			},

			&cli.Uint64Flag{
				Name:        "key_range",
				Aliases:     []string{"r"},
				Usage:       "Key range",
				Value:       100000000,
				Destination: &config.KeyRange,
			},
			&cli.Uint64Flag{
				Name:        "min_value_size",
				Aliases:     []string{"m"},
				Usage:       "Minimum value size",
				Value:       2,
				Destination: &config.MinValueSize,
			},
			&cli.Uint64Flag{
				Name:        "max_value_size",
				Aliases:     []string{"M"},
				Usage:       "Maximum value size",
				Value:       32,
				Destination: &config.MaxValueSize,
			},
			&cli.Float64Flag{
				Name:        "delete_ratio",
				Aliases:     []string{"dr"},
				Usage:       "Delete ratio",
				Value:       0.3,
				Destination: &config.DeleteRatio,
			},
			&cli.DurationFlag{
				Name:    "runtime",
				Aliases: []string{"rt"},
				Value:   100 * time.Second,
				Usage:   "Duration to run the benchmark",
			},
			&cli.BoolFlag{
				Name:    "init_mode",
				Aliases: []string{"im"},
				Value:   false,
				Usage:   "init data mode for preparing the test data",
			},
			&cli.Float64Flag{
				Name:        "rw_ratio",
				Aliases:     []string{"rr"},
				Usage:       "write and read ratio",
				Value:       0.01,
				Destination: &config.RwRatio,
			},
		},
		Action: func(c *cli.Context) error {
			fmt.Printf("Running with config: %+v\n", config)
			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func runPerf(c *cli.Context) error {
	var stateDB TrieDatabase
	engine := c.String("engine")
	if engine == PbssRawTrieEngine {
		fmt.Println("start to test trie:", PbssRawTrieEngine)
		dir, _ := os.Getwd()
		stateDB = OpenPbssDB(filepath.Join(dir, "pbss-dir"), types.EmptyRootHash)
	} else if engine == VERSADBEngine {
		fmt.Println("start to test trie:", VERSADBEngine)
		stateDB = OpenVersaTrie(0, nil)
	} else if engine == StateTrieEngine {
		dir, _ := os.Getwd()
		stateDB = OpenStateTrie(filepath.Join(dir, "state-trie-dir"), types.EmptyRootHash)
	}
	runner := NewRunner(stateDB, parsePerfConfig(c), 1000)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	defer cancel()

	address := net.JoinHostPort(c.String("metrics.addr"), fmt.Sprintf("%d", c.Int("metrics.port")))
	fmt.Println("Enabling stand-alone metrics HTTP endpoint", "address", address)
	exp.Setup(address)

	go metrics.CollectProcessMetrics(3 * time.Second)
	runner.Run(ctx)
	return nil
}

func runPerfDB(c *cli.Context) error {
	var stateDB StateDatabase
	engine := c.String("engine")
	if engine == VERSADBEngine {
		fmt.Println("start to test trie:", VERSADBEngine)
		stateDB = OpenVersaDB("versa-db", -1)
	} else if engine == StateTrieEngine {
		dir, _ := os.Getwd()
		stateDB = NewStateRunner(filepath.Join(dir, "state-trie-dir"), types.EmptyRootHash)
	}
	runner := NewDBRunner(stateDB, parsePerfConfig(c), 1000)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	defer cancel()

	address := net.JoinHostPort(c.String("metrics.addr"), fmt.Sprintf("%d", c.Int("metrics.port")))
	fmt.Println("Enabling stand-alone metrics HTTP endpoint", "address", address)
	exp.Setup(address)

	// http.HandleFunc("/debug/pprof/heap", pprof.Index)
	go func() {
		http.ListenAndServe(":6061", nil)
	}()

	go metrics.CollectProcessMetrics(3 * time.Second)
	runner.Run(ctx)
	return nil
}

func verifyHash(c *cli.Context) error {
	dir, _ := os.Getwd()
	secureTrie := OpenStateTrie(filepath.Join(dir, "test-dir"), types.EmptyRootHash)
	versaTrie := OpenVersaTrie(0, nil)

	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
	}

	for _, val := range vals {
		secureTrie.Put([]byte(val.k), []byte(val.v))
		versaTrie.Put([]byte(val.k), []byte(val.v))
	}
	hash1 := versaTrie.Hash()
	hash2, _ := secureTrie.Commit()
	if hash1 != hash2 {
		fmt.Printf("compare hash root not same, pbss root %v, versa root %v \n",
			hash2, hash1)
		panic("basic test fail")
	}

	secureTrie.Delete([]byte("doge"))
	versaTrie.Delete([]byte("doge"))
	hash1 = versaTrie.Hash()
	hash2, _ = secureTrie.Commit()
	if hash1 != hash2 {
		fmt.Printf("compare hash root not same, pbss root %v, versa root %v \n",
			hash2, hash1)
		panic("basic test fail")
	}

	verifyer := NewVerifyer(secureTrie, versaTrie, parsePerfConfig(c), 10)
	ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	defer cancel()
	fmt.Println("begin to verify root hash, the batch size of block is", verifyer.perfConfig.BatchSize)
	verifyer.Run(ctx)
	return nil
}

func verifyDB(c *cli.Context) error {
	VersaDB := OpenVersaDB("versa-db", -1)
	dir, _ := os.Getwd()
	stateDB := NewStateRunner(filepath.Join(dir, "state-trie-dir"), types.EmptyRootHash)

	//ctx, cancel := context.WithTimeout(context.Background(), c.Duration("runtime"))
	//defer cancel()
	verifyer := NewStateVerifyer(stateDB, VersaDB, parsePerfConfig(c), 10)

	fmt.Println("begin to verify root hash, the batch size of block is", verifyer.perfConfig.BatchSize)

	//	verifyer.Run(ctx)
	return nil
}

func parsePerfConfig(c *cli.Context) PerfConfig {
	batchSize := c.Uint64("bs")
	threadNum := c.Int("threads")
	keyRange := c.Uint64("key_range")
	maxValueSize := c.Uint64("max_value_size")
	minValueSize := c.Uint64("min_value_size")
	deleteRatio := c.Float64("delete_ratio")
	rwRatio := c.Float64("rw_ratio")
	trieSize := c.Uint64("triesize")
	smallTrieSize := c.Uint64("smallstorage")
	trieNum := c.Uint64("trienum")
	largeTrieNum := c.Uint64("largetries")
	accounts := c.Uint64("accounts")
	accountBlock := c.Uint64("account_block")
	trieBlock := c.Uint64("trie_block")
	initmodeFlag := c.Bool("init_mode")
	sleepTime := c.Uint64("sleeptime")
	return PerfConfig{
		BatchSize:        batchSize,
		NumJobs:          threadNum,
		KeyRange:         keyRange,
		MinValueSize:     minValueSize,
		MaxValueSize:     maxValueSize,
		DeleteRatio:      deleteRatio,
		RwRatio:          rwRatio,
		StorageTrieSize:  trieSize,
		SmallStorageSize: smallTrieSize,
		AccountsInitSize: accounts,
		AccountsBlocks:   accountBlock,
		TrieBlocks:       trieBlock,
		StorageTrieNum:   trieNum,
		LargeTrieNum:     largeTrieNum,
		IsInitMode:       initmodeFlag,
		SleepTime:        sleepTime,
	}
}
