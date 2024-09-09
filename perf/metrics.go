package main

import "github.com/ethereum/go-ethereum/metrics"

var (
	stateTrieGetLatency  = metrics.NewRegisteredTimer("statetrie/get/latency", nil)
	stateTriePutLatency  = metrics.NewRegisteredTimer("statetrie/put/latency", nil)
	VeraTrieGetLatency   = metrics.NewRegisteredTimer("veraTrie/get/latency", nil)
	VeraTriePutLatency   = metrics.NewRegisteredTimer("veraTrie/put/latency", nil)
	stateTrieHashLatency = metrics.NewRegisteredTimer("statetrie/hash/latency", nil)
	VeraTrieHashLatency  = metrics.NewRegisteredTimer("veraTrie/hash/latency", nil)

	stateDBCommitLatency = metrics.NewRegisteredTimer("statedb/commit/latency", nil)
	VeraDBCommitLatency  = metrics.NewRegisteredTimer("veradb/commit/latency", nil)

	stateDBRWLatency = metrics.NewRegisteredTimer("statedb/rw/latency", nil)
	VeraDBRWLatency  = metrics.NewRegisteredTimer("veradb/rw/latency", nil)

	stateDBImportLatency = metrics.NewRegisteredTimer("statedb/import/latency", nil)
	VeraDBImportLatency  = metrics.NewRegisteredTimer("veradb/import/latency", nil)

	stateDBHashLatency = metrics.NewRegisteredTimer("statedb/hash/latency", nil)
	VeraDBHashLatency  = metrics.NewRegisteredTimer("veradb/hash/latency", nil)

	StateDBGetLatency        = metrics.NewRegisteredTimer("statedb/get/latency", nil)
	StateDBAccPutLatency     = metrics.NewRegisteredTimer("statedb/account/put/latency", nil)
	StateDBStoragePutLatency = metrics.NewRegisteredTimer("statedb/storage/put/latency", nil)
	StateDBAccGetLatency     = metrics.NewRegisteredTimer("statedb/account/get/latency", nil)
	StateDBStorageGetLatency = metrics.NewRegisteredTimer("statedb/storage/get/latency", nil)

	VersaDBAccGetLatency     = metrics.NewRegisteredTimer("versadb/account/get/latency", nil)
	versaDBStorageGetLatency = metrics.NewRegisteredTimer("versadb/storage/get/latency", nil)
	VersaDBAccPutLatency     = metrics.NewRegisteredTimer("versadb/account/put/latency", nil)
	versaDBStoragePutLatency = metrics.NewRegisteredTimer("versadb/storage/put/latency", nil)

	stateTrieMemoryUsage = metrics.NewRegisteredGauge("statetrie/memory/usage", nil)
	VeraTrieMemoryUsage  = metrics.NewRegisteredGauge("veraTrie/memory/usage", nil)
	stateTrieGetTps      = metrics.NewRegisteredGauge("statetrie/get/tps", nil)
	stateTriePutTps      = metrics.NewRegisteredGauge("statetrie/put/tps", nil)
	VeraTrieGetTps       = metrics.NewRegisteredGauge("veraTrie/get/tps", nil)
	VeraTriePutTps       = metrics.NewRegisteredGauge("veraTrie/put/tps", nil)
	BlockHeight          = metrics.NewRegisteredGauge("versa/block/height", nil)

	stateDBGetTps = metrics.NewRegisteredGauge("statedb/get/tps", nil)
	stateDBPutTps = metrics.NewRegisteredGauge("statedb/put/tps", nil)
	VeraDBGetTps  = metrics.NewRegisteredGauge("veradb/get/tps", nil)
	VeraDBPutTps  = metrics.NewRegisteredGauge("veradb/put/tps", nil)

	failGetCount   = metrics.NewRegisteredCounter("db/get/fail", nil)
	failWriteCount = metrics.NewRegisteredCounter("db/put/fail", nil)
)
