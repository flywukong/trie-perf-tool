package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/urfave/cli/v2"
)

const (
	PBSSEngine = "mpt-pbss"
)

// PerfConfig struct to hold command line arguments
type PerfConfig struct {
	Engine       string
	DataDir      string
	BatchSize    uint64
	NumJobs      int
	KeyRange     uint64
	MinValueSize uint64
	MaxValueSize uint64
	DeleteRatio  float64
}

const version = "1.0.0"

// Run is the function to run the bsperftool command line tool
func main() {
	var config PerfConfig

	app := &cli.App{
		Name:    "perftool",
		Usage:   "A tool to perform state db benchmarking",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:  "put",
				Usage: "Put random keys into the trie database",
				Action: func(c *cli.Context) error {
					run(c)
					return nil
				},
			},
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "engine",
				Aliases:     []string{"e"},
				Usage:       "Engine to use",
				Destination: &config.Engine,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "datadir",
				Aliases:     []string{"d"},
				Usage:       "Data directory",
				Value:       "./dataset",
				Destination: &config.DataDir,
			},
			&cli.Uint64Flag{
				Name:        "bs",
				Aliases:     []string{"b"},
				Usage:       "Batch size",
				Value:       3000,
				Destination: &config.BatchSize,
			},
			&cli.IntFlag{
				Name:        "threads",
				Aliases:     []string{"t"},
				Usage:       "Number of jobs",
				Value:       10,
				Destination: &config.NumJobs,
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
				Value:       300,
				Destination: &config.MinValueSize,
			},
			&cli.Uint64Flag{
				Name:        "max_value_size",
				Aliases:     []string{"M"},
				Usage:       "Maximum value size",
				Value:       300,
				Destination: &config.MaxValueSize,
			},
			&cli.Float64Flag{
				Name:        "delete_ratio",
				Aliases:     []string{"dr"},
				Usage:       "Delete ratio",
				Value:       0.3,
				Destination: &config.DeleteRatio,
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

func run(c *cli.Context) error {

	var stateDB TrieDatabase
	engine := c.String("engine")
	if engine == PBSSEngine {
		fmt.Println("start to test", PBSSEngine)
		dir, _ := os.Getwd()
		stateDB = OpenDB(filepath.Join(dir, "testdir"), types.EmptyRootHash)
		batchSize := c.Int64("bs")
		jobNum := c.Int("threads")
		keyRange := c.Uint64("key_range")
		maxValueSize := c.Uint64("max_value_size")
		minValueSize := c.Uint64("min_value_size")
		deleteRatio := c.Float64("delete_ratio")

		runner := NewRunner(stateDB, uint64(batchSize), jobNum, keyRange, minValueSize, maxValueSize, deleteRatio, 10)
		fmt.Println("begin to press test")
		runner.Run()
	}
	return nil
}

/*
	func run(numThreads int) {
		taskCh := make(chan []byte, 1800)
		var wg sync.WaitGroup
		var completedTasks int64

		atomic.StoreInt64(&completedTasks, 0)
		for i := 0; i < numThreads; i++ {
			wg.Add(1)
			go worker(db, taskCh, &wg, &completedTasks)
		}

		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				atomic.StoreInt64(&completedTasks, 0)
				for i := 0; i < 1800; i++ {
					taskCh <- generateRandomKey()
				}
				// Wait for the batch to complete
				time.Sleep(3 * time.Second)
				// Print progress
				fmt.Printf("Completed %d out of 1800 tasks\n", atomic.LoadInt64(&completedTasks))
			}
		}
	}

	func worker(db TrieDatabase, taskCh <-chan []byte, wg *sync.WaitGroup, completedTasks *int64) {
		defer wg.Done()
		for key := range taskCh {
			value := generateRandomValue()
			err := db.Put(key, value)
			if err != nil {
				log.Printf("Error putting key: %v", err)
			}
		}
		atomic.AddInt64(completedTasks, 1)
	}
*/
func generateRandomKey() []byte {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		log.Fatal("Error generating random key:", err)
	}
	return key
}

func generateRandomValue() []byte {
	value := make([]byte, 64)
	_, err := rand.Read(value)
	if err != nil {
		log.Fatal("Error generating random value:", err)
	}
	return value
}
