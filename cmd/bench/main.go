package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/raafay/kvstore/engine"
)

func main() {
	dir := flag.String("dir", "", "data directory (default: temp dir)")
	numKeys := flag.Int("n", 100000, "number of keys")
	valueSize := flag.Int("vsize", 100, "value size in bytes")
	flag.Parse()

	dataDir := *dir
	if dataDir == "" {
		var err error
		dataDir, err = os.MkdirTemp("", "kvbench-*")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
			os.Exit(1)
		}
		defer os.RemoveAll(dataDir)
	}

	eng, err := engine.Open(engine.Options{
		Dir:          dataDir,
		MemtableSize: 4 * 1024 * 1024,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open engine: %v\n", err)
		os.Exit(1)
	}
	defer eng.Close()

	n := *numKeys
	vsize := *valueSize
	value := make([]byte, vsize)
	rand.Read(value)

	fmt.Printf("Benchmark: %d keys, %d byte values\n\n", n, vsize)

	// Generate keys
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%010d", i))
	}

	// Shuffle for random writes
	shuffled := make([][]byte, n)
	copy(shuffled, keys)
	rand.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

	// --- Sequential Writes ---
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := eng.Put(keys[i], value); err != nil {
			fmt.Fprintf(os.Stderr, "put error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Sequential Writes: %d ops in %v (%.0f ops/sec)\n", n, elapsed, float64(n)/elapsed.Seconds())

	// --- Random Reads ---
	rand.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	start = time.Now()
	for i := 0; i < n; i++ {
		if _, err := eng.Get(keys[i]); err != nil && err != engine.ErrNotFound {
			fmt.Fprintf(os.Stderr, "get error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("Random Reads:      %d ops in %v (%.0f ops/sec)\n", n, elapsed, float64(n)/elapsed.Seconds())

	// --- Random Writes (overwrite) ---
	start = time.Now()
	for i := 0; i < n; i++ {
		if err := eng.Put(shuffled[i], value); err != nil {
			fmt.Fprintf(os.Stderr, "put error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("Random Writes:     %d ops in %v (%.0f ops/sec)\n", n, elapsed, float64(n)/elapsed.Seconds())

	// --- Batch Writes ---
	batchSize := 100
	batches := n / batchSize
	batchKeys := make([][]byte, batchSize)
	batchVals := make([][]byte, batchSize)
	start = time.Now()
	for b := 0; b < batches; b++ {
		for i := 0; i < batchSize; i++ {
			batchKeys[i] = []byte(fmt.Sprintf("batch-%010d", b*batchSize+i))
			batchVals[i] = value
		}
		if err := eng.BatchPut(batchKeys, batchVals); err != nil {
			fmt.Fprintf(os.Stderr, "batch put error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed = time.Since(start)
	totalOps := batches * batchSize
	fmt.Printf("Batch Writes:      %d ops in %v (%.0f ops/sec, batch size %d)\n", totalOps, elapsed, float64(totalOps)/elapsed.Seconds(), batchSize)

	// --- Range Reads ---
	rangeCount := 1000
	rangeSize := 100
	start = time.Now()
	for i := 0; i < rangeCount; i++ {
		startIdx := rand.Intn(n - rangeSize)
		startKey := []byte(fmt.Sprintf("key-%010d", startIdx))
		endKey := []byte(fmt.Sprintf("key-%010d", startIdx+rangeSize))
		if _, err := eng.ReadRange(startKey, endKey); err != nil {
			fmt.Fprintf(os.Stderr, "range error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("Range Reads:       %d scans in %v (%.0f scans/sec, ~%d keys each)\n", rangeCount, elapsed, float64(rangeCount)/elapsed.Seconds(), rangeSize)

	// --- Delete ---
	deleteCount := n / 10
	start = time.Now()
	for i := 0; i < deleteCount; i++ {
		if err := eng.Delete(keys[i]); err != nil {
			fmt.Fprintf(os.Stderr, "delete error: %v\n", err)
			os.Exit(1)
		}
	}
	elapsed = time.Since(start)
	fmt.Printf("Deletes:           %d ops in %v (%.0f ops/sec)\n", deleteCount, elapsed, float64(deleteCount)/elapsed.Seconds())

	fmt.Println("\nDone.")
}
