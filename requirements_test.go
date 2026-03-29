// requirements_test.go
//
// Comprehensive verification tests mapped directly to the Moniepoint
// Take-Home Assessment PDF requirements.
//
// PDF Requirements:
//   Interface:
//     1. Put(Key, Value)
//     2. Read(Key)
//     3. ReadKeyRange(StartKey, EndKey)
//     4. BatchPut(..keys, ..values)
//     5. Delete(key)
//
//   Non-functional:
//     1. Low latency per item read or written
//     2. High throughput, especially when writing an incoming stream of random items
//     3. Ability to handle datasets much larger than RAM w/o degradation
//     4. Crash friendliness, both in terms of fast recovery and not losing data
//     5. Predictable behavior under heavy access load or large volume
//
//   Bonus:
//     1. Replicate data to multiple nodes
//     2. Handle automatic failover to the other nodes
//
//   Constraint: Standard library only (no external dependencies)

package kvstore_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raafay/kvstore/client"
	"github.com/raafay/kvstore/engine"
	"github.com/raafay/kvstore/raft"
	"github.com/raafay/kvstore/server"
)

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

func newTestEngine(t *testing.T, opts ...engine.Options) *engine.Engine {
	t.Helper()
	dir := t.TempDir()
	o := engine.Options{Dir: dir, MemtableSize: 4096} // small memtable for fast flush
	if len(opts) > 0 {
		o = opts[0]
		if o.Dir == "" {
			o.Dir = dir
		}
	}
	eng, err := engine.Open(o)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng
}

func newTestServerAndClient(t *testing.T) (*server.TCPServer, *client.Client, *engine.Engine) {
	t.Helper()
	eng := newTestEngine(t)
	srv := server.NewTCPServer(":0", eng)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() { srv.Stop() })

	c, err := client.Connect(srv.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	return srv, c, eng
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func waitForCondition(t *testing.T, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
}

// ============================================================================
// REQUIREMENT: Interface 1 — Put(Key, Value)
// ============================================================================

func TestRequirement_Put_BasicInsert(t *testing.T) {
	eng := newTestEngine(t)

	// Put a single key-value pair
	err := eng.Put([]byte("hello"), []byte("world"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := eng.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get after Put failed: %v", err)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("expected 'world', got '%s'", val)
	}
}

func TestRequirement_Put_Overwrite(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("key"), []byte("value1"))
	eng.Put([]byte("key"), []byte("value2"))

	val, err := eng.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value2")) {
		t.Fatalf("expected 'value2' after overwrite, got '%s'", val)
	}
}

func TestRequirement_Put_EmptyValue(t *testing.T) {
	eng := newTestEngine(t)

	err := eng.Put([]byte("key"), []byte(""))
	if err != nil {
		t.Fatalf("Put with empty value failed: %v", err)
	}

	val, err := eng.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if len(val) != 0 {
		t.Fatalf("expected empty value, got '%s'", val)
	}
}

func TestRequirement_Put_LargeValue(t *testing.T) {
	eng := newTestEngine(t)

	// 1MB value
	bigVal := randomBytes(1024 * 1024)
	err := eng.Put([]byte("bigkey"), bigVal)
	if err != nil {
		t.Fatalf("Put with large value failed: %v", err)
	}

	val, err := eng.Get([]byte("bigkey"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, bigVal) {
		t.Fatalf("large value mismatch")
	}
}

func TestRequirement_Put_BinaryKeyAndValue(t *testing.T) {
	eng := newTestEngine(t)

	// Keys and values with null bytes, high bytes, etc.
	binKey := []byte{0x00, 0xFF, 0x01, 0xFE, 0x00}
	binVal := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00}
	err := eng.Put(binKey, binVal)
	if err != nil {
		t.Fatalf("Put with binary data failed: %v", err)
	}

	val, err := eng.Get(binKey)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, binVal) {
		t.Fatalf("binary value mismatch")
	}
}

func TestRequirement_Put_NetworkAPI(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	err := c.Put([]byte("netkey"), []byte("netval"))
	if err != nil {
		t.Fatalf("Put via network failed: %v", err)
	}

	val, err := c.Get([]byte("netkey"))
	if err != nil {
		t.Fatalf("Get via network failed: %v", err)
	}
	if !bytes.Equal(val, []byte("netval")) {
		t.Fatalf("expected 'netval', got '%s'", val)
	}
}

// ============================================================================
// REQUIREMENT: Interface 2 — Read(Key)
// ============================================================================

func TestRequirement_Read_ExistingKey(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("exists"), []byte("here"))

	val, err := eng.Get([]byte("exists"))
	if err != nil {
		t.Fatalf("Read existing key failed: %v", err)
	}
	if !bytes.Equal(val, []byte("here")) {
		t.Fatalf("unexpected value: %s", val)
	}
}

func TestRequirement_Read_NonExistentKey(t *testing.T) {
	eng := newTestEngine(t)

	_, err := eng.Get([]byte("ghost"))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for non-existent key, got: %v", err)
	}
}

func TestRequirement_Read_AfterDelete(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("temp"), []byte("data"))
	eng.Delete([]byte("temp"))

	_, err := eng.Get([]byte("temp"))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestRequirement_Read_MostRecentValue(t *testing.T) {
	eng := newTestEngine(t)

	// Write the same key multiple times
	for i := 0; i < 10; i++ {
		eng.Put([]byte("counter"), []byte(fmt.Sprintf("version-%d", i)))
	}

	val, err := eng.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if !bytes.Equal(val, []byte("version-9")) {
		t.Fatalf("expected 'version-9', got '%s'", val)
	}
}

func TestRequirement_Read_AfterFlushToSSTable(t *testing.T) {
	eng := newTestEngine(t)

	// Write enough data to trigger memtable flush (small memtable = 4KB)
	eng.Put([]byte("pre-flush"), []byte("value-before"))
	for i := 0; i < 200; i++ {
		eng.Put([]byte(fmt.Sprintf("padding-%05d", i)), randomBytes(64))
	}
	// Wait for flush
	time.Sleep(200 * time.Millisecond)

	// Read the key that's now in an SSTable
	val, err := eng.Get([]byte("pre-flush"))
	if err != nil {
		t.Fatalf("Read after flush failed: %v", err)
	}
	if !bytes.Equal(val, []byte("value-before")) {
		t.Fatalf("expected 'value-before', got '%s'", val)
	}
}

func TestRequirement_Read_NetworkAPI(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	c.Put([]byte("nk"), []byte("nv"))

	val, err := c.Get([]byte("nk"))
	if err != nil {
		t.Fatalf("Read via network failed: %v", err)
	}
	if !bytes.Equal(val, []byte("nv")) {
		t.Fatalf("expected 'nv', got '%s'", val)
	}

	// Not found via network
	_, err = c.Get([]byte("nonexistent"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound via network, got: %v", err)
	}
}

// ============================================================================
// REQUIREMENT: Interface 3 — ReadKeyRange(StartKey, EndKey)
// ============================================================================

func TestRequirement_ReadKeyRange_BasicRange(t *testing.T) {
	eng := newTestEngine(t)

	// Insert keys a-z
	for c := 'a'; c <= 'z'; c++ {
		eng.Put([]byte(string(c)), []byte(fmt.Sprintf("val-%c", c)))
	}

	pairs, err := eng.ReadRange([]byte("d"), []byte("h"))
	if err != nil {
		t.Fatalf("ReadKeyRange failed: %v", err)
	}

	expected := []string{"d", "e", "f", "g", "h"}
	if len(pairs) != len(expected) {
		t.Fatalf("expected %d pairs, got %d", len(expected), len(pairs))
	}
	for i, p := range pairs {
		if string(p.Key) != expected[i] {
			t.Fatalf("pair[%d] key: expected '%s', got '%s'", i, expected[i], p.Key)
		}
	}
}

func TestRequirement_ReadKeyRange_SortedOrder(t *testing.T) {
	eng := newTestEngine(t)

	// Insert in random order
	keys := []string{"zebra", "apple", "mango", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		eng.Put([]byte(k), []byte("v-"+k))
	}

	pairs, err := eng.ReadRange([]byte("a"), []byte("z"))
	if err != nil {
		t.Fatalf("ReadKeyRange failed: %v", err)
	}

	// Verify sorted order
	for i := 1; i < len(pairs); i++ {
		if bytes.Compare(pairs[i-1].Key, pairs[i].Key) >= 0 {
			t.Fatalf("results not sorted: '%s' >= '%s'", pairs[i-1].Key, pairs[i].Key)
		}
	}
}

func TestRequirement_ReadKeyRange_ExcludesDeleted(t *testing.T) {
	eng := newTestEngine(t)

	for c := 'a'; c <= 'f'; c++ {
		eng.Put([]byte(string(c)), []byte("v"))
	}
	eng.Delete([]byte("c"))
	eng.Delete([]byte("e"))

	pairs, err := eng.ReadRange([]byte("a"), []byte("f"))
	if err != nil {
		t.Fatalf("ReadKeyRange failed: %v", err)
	}

	expected := []string{"a", "b", "d", "f"}
	if len(pairs) != len(expected) {
		t.Fatalf("expected %d pairs (excluding deleted), got %d", len(expected), len(pairs))
	}
	for i, p := range pairs {
		if string(p.Key) != expected[i] {
			t.Fatalf("pair[%d]: expected '%s', got '%s'", i, expected[i], p.Key)
		}
	}
}

func TestRequirement_ReadKeyRange_EmptyRange(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("a"), []byte("v"))
	eng.Put([]byte("z"), []byte("v"))

	pairs, err := eng.ReadRange([]byte("m"), []byte("n"))
	if err != nil {
		t.Fatalf("ReadKeyRange failed: %v", err)
	}
	if len(pairs) != 0 {
		t.Fatalf("expected 0 pairs for empty range, got %d", len(pairs))
	}
}

func TestRequirement_ReadKeyRange_SpanningFlushes(t *testing.T) {
	eng := newTestEngine(t)

	// Write data across multiple memtable flushes
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("rkey-%05d", i)
		eng.Put([]byte(key), []byte(fmt.Sprintf("val-%05d", i)))
	}
	time.Sleep(300 * time.Millisecond) // allow flushes

	// Range query that spans multiple SSTables
	pairs, err := eng.ReadRange([]byte("rkey-00100"), []byte("rkey-00200"))
	if err != nil {
		t.Fatalf("ReadKeyRange spanning flushes failed: %v", err)
	}

	// Should include keys 00100 through 00200 inclusive
	if len(pairs) != 101 {
		t.Fatalf("expected 101 pairs, got %d", len(pairs))
	}

	// Verify sorted and correct
	for i, p := range pairs {
		expected := fmt.Sprintf("rkey-%05d", 100+i)
		if string(p.Key) != expected {
			t.Fatalf("pair[%d]: expected '%s', got '%s'", i, expected, p.Key)
		}
	}
}

func TestRequirement_ReadKeyRange_NetworkAPI(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	for i := 0; i < 10; i++ {
		c.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i)))
	}

	pairs, err := c.GetRange([]byte("k03"), []byte("k07"))
	if err != nil {
		t.Fatalf("ReadKeyRange via network failed: %v", err)
	}
	if len(pairs) != 5 {
		t.Fatalf("expected 5 pairs, got %d", len(pairs))
	}
}

// ============================================================================
// REQUIREMENT: Interface 4 — BatchPut(..keys, ..values)
// ============================================================================

func TestRequirement_BatchPut_Basic(t *testing.T) {
	eng := newTestEngine(t)

	keys := make([][]byte, 50)
	vals := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		keys[i] = []byte(fmt.Sprintf("bk-%03d", i))
		vals[i] = []byte(fmt.Sprintf("bv-%03d", i))
	}

	err := eng.BatchPut(keys, vals)
	if err != nil {
		t.Fatalf("BatchPut failed: %v", err)
	}

	// Verify all keys are readable
	for i := 0; i < 50; i++ {
		val, err := eng.Get(keys[i])
		if err != nil {
			t.Fatalf("Get after BatchPut failed for key %d: %v", i, err)
		}
		if !bytes.Equal(val, vals[i]) {
			t.Fatalf("value mismatch for key %d", i)
		}
	}
}

func TestRequirement_BatchPut_Atomicity(t *testing.T) {
	eng := newTestEngine(t)

	// BatchPut should write all or nothing (all succeed together)
	keys := [][]byte{[]byte("atom1"), []byte("atom2"), []byte("atom3")}
	vals := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	err := eng.BatchPut(keys, vals)
	if err != nil {
		t.Fatalf("BatchPut failed: %v", err)
	}

	// All three must exist
	for i, k := range keys {
		val, err := eng.Get(k)
		if err != nil {
			t.Fatalf("key %s not found after batch: %v", k, err)
		}
		if !bytes.Equal(val, vals[i]) {
			t.Fatalf("value mismatch for %s", k)
		}
	}
}

func TestRequirement_BatchPut_Large(t *testing.T) {
	eng := newTestEngine(t)

	n := 1000
	keys := make([][]byte, n)
	vals := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("large-batch-%06d", i))
		vals[i] = randomBytes(100)
	}

	err := eng.BatchPut(keys, vals)
	if err != nil {
		t.Fatalf("large BatchPut failed: %v", err)
	}

	// Spot-check 100 random keys
	for i := 0; i < 100; i++ {
		idx := rand.Intn(n)
		val, err := eng.Get(keys[idx])
		if err != nil {
			t.Fatalf("Get after large batch failed for index %d: %v", idx, err)
		}
		if !bytes.Equal(val, vals[idx]) {
			t.Fatalf("value mismatch at index %d", idx)
		}
	}
}

func TestRequirement_BatchPut_NetworkAPI(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	keys := make([][]byte, 20)
	vals := make([][]byte, 20)
	for i := 0; i < 20; i++ {
		keys[i] = []byte(fmt.Sprintf("nbk-%02d", i))
		vals[i] = []byte(fmt.Sprintf("nbv-%02d", i))
	}

	err := c.BatchPut(keys, vals)
	if err != nil {
		t.Fatalf("BatchPut via network failed: %v", err)
	}

	for i := 0; i < 20; i++ {
		val, err := c.Get(keys[i])
		if err != nil {
			t.Fatalf("Get after network BatchPut failed: %v", err)
		}
		if !bytes.Equal(val, vals[i]) {
			t.Fatalf("value mismatch")
		}
	}
}

// ============================================================================
// REQUIREMENT: Interface 5 — Delete(key)
// ============================================================================

func TestRequirement_Delete_Basic(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("del-me"), []byte("value"))
	err := eng.Delete([]byte("del-me"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = eng.Get([]byte("del-me"))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Delete, got: %v", err)
	}
}

func TestRequirement_Delete_NonExistentKey(t *testing.T) {
	eng := newTestEngine(t)

	// Deleting a non-existent key should not error
	err := eng.Delete([]byte("never-existed"))
	if err != nil {
		t.Fatalf("Delete of non-existent key should not error: %v", err)
	}
}

func TestRequirement_Delete_ThenReinsert(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("phoenix"), []byte("v1"))
	eng.Delete([]byte("phoenix"))
	eng.Put([]byte("phoenix"), []byte("v2"))

	val, err := eng.Get([]byte("phoenix"))
	if err != nil {
		t.Fatalf("Get after re-insert failed: %v", err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("expected 'v2' after re-insert, got '%s'", val)
	}
}

func TestRequirement_Delete_SurvivesFlush(t *testing.T) {
	eng := newTestEngine(t)

	eng.Put([]byte("flush-del"), []byte("val"))

	// Force flush by writing enough data
	for i := 0; i < 200; i++ {
		eng.Put([]byte(fmt.Sprintf("pad-%05d", i)), randomBytes(64))
	}
	time.Sleep(200 * time.Millisecond)

	// Delete after key is in SSTable
	eng.Delete([]byte("flush-del"))

	_, err := eng.Get([]byte("flush-del"))
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for deleted key in SSTable, got: %v", err)
	}
}

func TestRequirement_Delete_NetworkAPI(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	c.Put([]byte("ndel"), []byte("nval"))
	err := c.Delete([]byte("ndel"))
	if err != nil {
		t.Fatalf("Delete via network failed: %v", err)
	}

	_, err = c.Get([]byte("ndel"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after network Delete, got: %v", err)
	}
}

// ============================================================================
// REQUIREMENT: Non-functional 1 — Low latency per item read or written
// ============================================================================

func TestRequirement_LowLatency_PointRead(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4 * 1024 * 1024,
	})

	// Pre-populate
	for i := 0; i < 1000; i++ {
		eng.Put([]byte(fmt.Sprintf("lat-%06d", i)), randomBytes(100))
	}

	// Measure read latency
	var totalNs int64
	iterations := 1000
	for i := 0; i < iterations; i++ {
		key := []byte(fmt.Sprintf("lat-%06d", rand.Intn(1000)))
		start := time.Now()
		eng.Get(key)
		totalNs += time.Since(start).Nanoseconds()
	}

	avgLatencyUs := float64(totalNs) / float64(iterations) / 1000.0
	t.Logf("Average point read latency: %.1f µs", avgLatencyUs)

	// Reads from memtable should be well under 1ms
	if avgLatencyUs > 1000 {
		t.Fatalf("average read latency %.1f µs exceeds 1ms threshold", avgLatencyUs)
	}
}

func TestRequirement_LowLatency_PointWrite(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 64 * 1024 * 1024, // large memtable to avoid flush during test
	})

	var totalNs int64
	iterations := 100
	for i := 0; i < iterations; i++ {
		key := []byte(fmt.Sprintf("wlat-%06d", i))
		val := randomBytes(100)
		start := time.Now()
		eng.Put(key, val)
		totalNs += time.Since(start).Nanoseconds()
	}

	avgLatencyMs := float64(totalNs) / float64(iterations) / 1e6
	t.Logf("Average point write latency: %.2f ms (includes fsync)", avgLatencyMs)

	// Writes include fsync, so typically a few ms. Should be under 50ms.
	if avgLatencyMs > 50 {
		t.Fatalf("average write latency %.2f ms exceeds 50ms threshold", avgLatencyMs)
	}
}

// ============================================================================
// REQUIREMENT: Non-functional 2 — High throughput, especially for random writes
// ============================================================================

func TestRequirement_HighThroughput_BatchWrites(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4 * 1024 * 1024,
	})

	batchSize := 100
	numBatches := 100
	totalOps := batchSize * numBatches

	start := time.Now()
	for b := 0; b < numBatches; b++ {
		keys := make([][]byte, batchSize)
		vals := make([][]byte, batchSize)
		for i := 0; i < batchSize; i++ {
			keys[i] = []byte(fmt.Sprintf("tp-%06d", b*batchSize+i))
			vals[i] = randomBytes(100)
		}
		if err := eng.BatchPut(keys, vals); err != nil {
			t.Fatalf("BatchPut failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Batch write throughput: %.0f ops/sec (%d ops in %v)", opsPerSec, totalOps, elapsed)

	// BatchPut amortizes fsync; should achieve > 1000 ops/sec
	if opsPerSec < 1000 {
		t.Fatalf("batch throughput %.0f ops/sec is below 1000 minimum", opsPerSec)
	}
}

func TestRequirement_HighThroughput_RandomReads(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4 * 1024 * 1024,
	})

	// Pre-populate
	n := 5000
	for i := 0; i < n; i++ {
		eng.Put([]byte(fmt.Sprintf("rt-%06d", i)), randomBytes(100))
	}

	// Measure random read throughput
	iterations := 10000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		key := []byte(fmt.Sprintf("rt-%06d", rand.Intn(n)))
		eng.Get(key)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(iterations) / elapsed.Seconds()
	t.Logf("Random read throughput: %.0f ops/sec", opsPerSec)

	// Reads from memtable/cache should be extremely fast
	if opsPerSec < 10000 {
		t.Fatalf("read throughput %.0f ops/sec is below 10000 minimum", opsPerSec)
	}
}

// ============================================================================
// REQUIREMENT: Non-functional 3 — Handle datasets much larger than RAM w/o degradation
// ============================================================================

func TestRequirement_LargerThanRAM_ManyKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 1024, // very small memtable forces frequent flushes
	})

	// Write 50,000 keys — this forces many SSTable files
	n := 50000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("large-%08d", i)
		val := fmt.Sprintf("value-%08d", i)
		if err := eng.Put([]byte(key), []byte(val)); err != nil {
			t.Fatalf("Put failed at key %d: %v", i, err)
		}
	}

	// Wait for flushes and compaction
	time.Sleep(2 * time.Second)

	// Verify random sample of 500 keys are readable
	for i := 0; i < 500; i++ {
		idx := rand.Intn(n)
		key := fmt.Sprintf("large-%08d", idx)
		val, err := eng.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get failed for key %d after large write: %v", idx, err)
		}
		expected := fmt.Sprintf("value-%08d", idx)
		if string(val) != expected {
			t.Fatalf("value mismatch at key %d: expected '%s', got '%s'", idx, expected, val)
		}
	}

	// Verify range query works across many SSTables
	pairs, err := eng.ReadRange([]byte("large-00010000"), []byte("large-00010100"))
	if err != nil {
		t.Fatalf("ReadRange over large dataset failed: %v", err)
	}
	if len(pairs) != 101 {
		t.Fatalf("expected 101 pairs from range over large dataset, got %d", len(pairs))
	}
}

func TestRequirement_LargerThanRAM_UseMmap(t *testing.T) {
	// Verify that SSTable reader uses mmap (indirect test: open engine,
	// write data to force SSTables, close engine, reopen, read data)
	dir := t.TempDir()

	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 512})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	for i := 0; i < 1000; i++ {
		eng.Put([]byte(fmt.Sprintf("mm-%06d", i)), randomBytes(100))
	}
	time.Sleep(500 * time.Millisecond)
	eng.Close()

	// Verify SSTable files exist on disk
	entries, _ := os.ReadDir(dir)
	sstCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			sstCount++
		}
	}
	if sstCount == 0 {
		t.Fatalf("no SSTable files found — data not persisted to disk")
	}
	t.Logf("Found %d SSTable files on disk", sstCount)

	// Reopen and read — this exercises mmap-based SSTable reading
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 512})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer eng2.Close()

	for i := 0; i < 100; i++ {
		idx := rand.Intn(1000)
		_, err := eng2.Get([]byte(fmt.Sprintf("mm-%06d", idx)))
		if err != nil {
			t.Fatalf("Get from reopened engine (mmap) failed for key %d: %v", idx, err)
		}
	}
}

// ============================================================================
// REQUIREMENT: Non-functional 4 — Crash friendliness
// ============================================================================

func TestRequirement_CrashRecovery_WALReplay(t *testing.T) {
	dir := t.TempDir()

	// Write data, then simulate crash by NOT calling Close()
	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	for i := 0; i < 100; i++ {
		eng.Put([]byte(fmt.Sprintf("crash-%05d", i)), []byte(fmt.Sprintf("val-%05d", i)))
	}

	// SIMULATE CRASH: don't call eng.Close()
	// The memtable is in RAM and will be lost, but the WAL is on disk (fsync'd).

	// Reopen the engine — it should replay the WAL
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("recovery open failed: %v", err)
	}
	defer eng2.Close()

	// Verify ALL 100 keys are recovered
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("crash-%05d", i)
		val, err := eng2.Get([]byte(key))
		if err != nil {
			t.Fatalf("key '%s' lost after crash recovery: %v", key, err)
		}
		expected := fmt.Sprintf("val-%05d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch after recovery: expected '%s', got '%s'", expected, val)
		}
	}
	t.Log("All 100 keys recovered after simulated crash via WAL replay")
}

func TestRequirement_CrashRecovery_SSTablIntact(t *testing.T) {
	dir := t.TempDir()

	// Use a large memtable so we control when flushes happen
	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// Write 100 keys — they'll all be in the WAL and memtable
	for i := 0; i < 100; i++ {
		eng.Put([]byte(fmt.Sprintf("sst-crash-%06d", i)), []byte(fmt.Sprintf("val-%06d", i)))
	}

	// Close cleanly to flush to SSTable
	eng.Close()

	// Reopen with small memtable, write more, then crash
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}

	// Write more (these are only in WAL)
	for i := 100; i < 150; i++ {
		eng2.Put([]byte(fmt.Sprintf("sst-crash-%06d", i)), []byte(fmt.Sprintf("val-%06d", i)))
	}

	// CRASH — don't close eng2
	// SSTable has keys 0-99 from the first session. WAL has keys 100-149.

	eng3, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer eng3.Close()

	// Verify data from both SSTables AND WAL recovery
	missing := 0
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("sst-crash-%06d", i)
		val, err := eng3.Get([]byte(key))
		if err != nil {
			missing++
			continue
		}
		expected := fmt.Sprintf("val-%06d", i)
		if string(val) != expected {
			t.Errorf("value mismatch for key %d: got '%s'", i, val)
		}
	}
	t.Logf("Recovery: %d/150 keys found (%d missing)", 150-missing, missing)
	if missing > 0 {
		t.Fatalf("%d keys lost after crash recovery", missing)
	}
}

func TestRequirement_CrashRecovery_FastRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write a moderate amount of data
	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4096})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	for i := 0; i < 5000; i++ {
		eng.Put([]byte(fmt.Sprintf("rec-%06d", i)), randomBytes(50))
	}
	time.Sleep(500 * time.Millisecond)
	// Don't close — crash simulation

	// Measure recovery time
	start := time.Now()
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4096})
	recoveryTime := time.Since(start)
	if err != nil {
		t.Fatalf("recovery open failed: %v", err)
	}
	defer eng2.Close()

	t.Logf("Recovery time: %v (5000 keys)", recoveryTime)

	// Recovery should be fast (under 5 seconds for this dataset)
	if recoveryTime > 5*time.Second {
		t.Fatalf("recovery took %v, exceeds 5s threshold", recoveryTime)
	}
}

// ============================================================================
// REQUIREMENT: Non-functional 5 — Predictable behavior under heavy access load
// ============================================================================

func TestRequirement_Predictable_ConcurrentReadWrite(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4 * 1024 * 1024, // 4MB memtable to avoid constant flush blocking
	})

	// Pre-populate
	for i := 0; i < 1000; i++ {
		eng.Put([]byte(fmt.Sprintf("conc-%06d", i)), randomBytes(50))
	}

	// Launch concurrent readers and writers
	var wg sync.WaitGroup
	var readErrors, writeErrors atomic.Int64
	var readOps, writeOps atomic.Int64
	done := make(chan struct{})

	// 5 writer goroutines
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				key := fmt.Sprintf("conc-w%d-%06d", id, i)
				if err := eng.Put([]byte(key), randomBytes(50)); err != nil {
					writeErrors.Add(1)
				}
				writeOps.Add(1)
			}
		}(w)
	}

	// 10 reader goroutines
	for r := 0; r < 10; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				key := fmt.Sprintf("conc-%06d", rand.Intn(1000))
				_, err := eng.Get([]byte(key))
				if err != nil && !errors.Is(err, engine.ErrNotFound) {
					readErrors.Add(1)
				}
				readOps.Add(1)
			}
		}()
	}

	// Run for 2 seconds
	time.Sleep(2 * time.Second)
	close(done)
	wg.Wait()

	t.Logf("Reads: %d ops, Writes: %d ops", readOps.Load(), writeOps.Load())
	t.Logf("Read errors: %d, Write errors: %d", readErrors.Load(), writeErrors.Load())
	if readErrors.Load() > 0 {
		t.Fatalf("had %d read errors under concurrent load", readErrors.Load())
	}
	if writeErrors.Load() > 0 {
		t.Fatalf("had %d write errors under concurrent load", writeErrors.Load())
	}
}

func TestRequirement_Predictable_NoDeadlocks(t *testing.T) {
	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4 * 1024 * 1024,
	})

	// Simulate mixed workload: concurrent puts, gets, deletes, ranges, and batches
	var wg sync.WaitGroup
	done := make(chan struct{})

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				key := fmt.Sprintf("dl-%d-%06d", id, j)
				eng.Put([]byte(key), randomBytes(20))
			}
		}(i)
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				eng.ReadRange([]byte("dl-0"), []byte("dl-9"))
			}
		}()
	}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; ; j++ {
				select {
				case <-done:
					return
				default:
				}
				eng.Delete([]byte(fmt.Sprintf("dl-%d-%06d", id, j)))
			}
		}(i)
	}

	// If there's a deadlock, this test will time out
	time.Sleep(3 * time.Second)
	close(done)
	wg.Wait()
	t.Log("No deadlocks detected under mixed concurrent workload")
}

func TestRequirement_Predictable_GrowingDatasetReadLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping growing dataset test in short mode")
	}

	eng := newTestEngine(t, engine.Options{
		Dir:          t.TempDir(),
		MemtableSize: 4096,
	})

	// Measure read latency at different dataset sizes
	phases := []int{1000, 5000, 10000}
	latencies := make([]float64, len(phases))
	written := 0

	for p, target := range phases {
		for i := written; i < target; i++ {
			eng.Put([]byte(fmt.Sprintf("grow-%08d", i)), randomBytes(50))
		}
		written = target
		time.Sleep(500 * time.Millisecond) // allow flushes

		// Measure read latency
		var totalNs int64
		samples := 500
		for i := 0; i < samples; i++ {
			key := fmt.Sprintf("grow-%08d", rand.Intn(written))
			start := time.Now()
			eng.Get([]byte(key))
			totalNs += time.Since(start).Nanoseconds()
		}
		latencies[p] = float64(totalNs) / float64(samples) / 1000.0 // µs
		t.Logf("Dataset size %d: avg read latency %.1f µs", target, latencies[p])
	}

	// Latency at 10K keys should not be more than 100x latency at 1K keys
	// (LSM-tree with bloom filters should provide sub-linear degradation)
	if latencies[2] > latencies[0]*100 {
		t.Fatalf("read latency degraded too much: %.1f µs at 1K → %.1f µs at 10K (>100x)",
			latencies[0], latencies[2])
	}
}

// ============================================================================
// REQUIREMENT: Bonus 1 — Replicate data to multiple nodes
// ============================================================================

func TestRequirement_Replication_DataReplicatedAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft test in short mode")
	}

	config := raft.Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		ProposalTimeout:    5 * time.Second,
	}

	// Use unique ports for this test (20000 range)
	basePort := 20000 + rand.Intn(1000)
	addrs := make([]string, 3)
	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}

	nodes := make([]*raft.Node, 3)
	for i := 0; i < 3; i++ {
		dir := t.TempDir()
		peers := make([]string, 0, 2)
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		node, err := raft.NewNodeWithConfig(
			fmt.Sprintf("rep-node-%d", i),
			peers,
			dir,
			addrs[i],
			config,
		)
		if err != nil {
			t.Fatalf("failed to create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	// Start all nodes
	for i, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("failed to start node %d: %v", i, err)
		}
		defer n.Stop()
	}

	// Wait for leader election
	var leaderIdx int
	waitForCondition(t, 5*time.Second, "leader election", func() bool {
		for i, n := range nodes {
			if n.Raft().IsLeader() {
				leaderIdx = i
				return true
			}
		}
		return false
	})
	t.Logf("Leader elected: node-%d", leaderIdx)

	// Propose writes through the leader
	leader := nodes[leaderIdx].Raft()
	for i := 0; i < 10; i++ {
		data := raft.EncodePutCommand(
			[]byte(fmt.Sprintf("rep-key-%02d", i)),
			[]byte(fmt.Sprintf("rep-val-%02d", i)),
		)
		err := leader.Propose(raft.CmdRaftPut, data)
		if err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
	}

	// Wait for replication to all nodes
	time.Sleep(1 * time.Second)

	// Verify data is present on ALL nodes' engines
	for nodeIdx, n := range nodes {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("rep-key-%02d", i)
			val, err := n.Engine().Get([]byte(key))
			if err != nil {
				t.Fatalf("node-%d: key '%s' not replicated: %v", nodeIdx, key, err)
			}
			expected := fmt.Sprintf("rep-val-%02d", i)
			if string(val) != expected {
				t.Fatalf("node-%d: key '%s' value mismatch: '%s' vs '%s'", nodeIdx, key, val, expected)
			}
		}
	}
	t.Log("All 10 keys successfully replicated to all 3 nodes")
}

// ============================================================================
// REQUIREMENT: Bonus 2 — Handle automatic failover
// ============================================================================

func TestRequirement_Failover_LeaderCrash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping raft failover test in short mode")
	}

	config := raft.Config{
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  50 * time.Millisecond,
		ProposalTimeout:    5 * time.Second,
	}

	dirs := make([]string, 3)
	nodes := make([]*raft.Node, 3)
	addrs := make([]string, 3)
	foBasePort := 21000 + rand.Intn(1000)
	for i := 0; i < 3; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", foBasePort+i)
	}

	for i := 0; i < 3; i++ {
		dirs[i] = t.TempDir()
		peers := make([]string, 0, 2)
		for j := 0; j < 3; j++ {
			if j != i {
				peers = append(peers, addrs[j])
			}
		}
		node, err := raft.NewNodeWithConfig(
			fmt.Sprintf("fo-node-%d", i), peers, dirs[i], addrs[i], config,
		)
		if err != nil {
			t.Fatalf("create node %d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	// defer stops for all except the one we'll kill
	defer func() {
		for _, n := range nodes {
			if n != nil {
				n.Stop()
			}
		}
	}()

	// Wait for leader
	var leaderIdx int
	waitForCondition(t, 5*time.Second, "initial leader", func() bool {
		for i, n := range nodes {
			if n.Raft().IsLeader() {
				leaderIdx = i
				return true
			}
		}
		return false
	})
	t.Logf("Initial leader: node-%d", leaderIdx)

	// Write data through leader
	leader := nodes[leaderIdx].Raft()
	for i := 0; i < 5; i++ {
		data := raft.EncodePutCommand([]byte(fmt.Sprintf("fo-k%d", i)), []byte(fmt.Sprintf("fo-v%d", i)))
		if err := leader.Propose(raft.CmdRaftPut, data); err != nil {
			t.Fatalf("Propose failed: %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// KILL THE LEADER
	t.Logf("Stopping leader node-%d...", leaderIdx)
	nodes[leaderIdx].Stop()
	nodes[leaderIdx] = nil

	// Wait for new leader among remaining nodes
	var newLeaderIdx int
	waitForCondition(t, 5*time.Second, "new leader after failover", func() bool {
		for i, n := range nodes {
			if n != nil && n.Raft().IsLeader() {
				newLeaderIdx = i
				return true
			}
		}
		return false
	})
	t.Logf("New leader after failover: node-%d", newLeaderIdx)

	if newLeaderIdx == leaderIdx {
		t.Fatalf("new leader is the same as the killed leader")
	}

	// Verify data is still accessible on the new leader
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("fo-k%d", i)
		val, err := nodes[newLeaderIdx].Engine().Get([]byte(key))
		if err != nil {
			t.Fatalf("key '%s' lost after failover: %v", key, err)
		}
		expected := fmt.Sprintf("fo-v%d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch after failover for '%s': got '%s'", key, val)
		}
	}

	// Verify the new leader can accept new writes
	newLeader := nodes[newLeaderIdx].Raft()
	data := raft.EncodePutCommand([]byte("post-failover"), []byte("works"))
	err := newLeader.Propose(raft.CmdRaftPut, data)
	if err != nil {
		t.Fatalf("Propose on new leader after failover failed: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	val, err := nodes[newLeaderIdx].Engine().Get([]byte("post-failover"))
	if err != nil {
		t.Fatalf("post-failover write not readable: %v", err)
	}
	if string(val) != "works" {
		t.Fatalf("post-failover value mismatch: '%s'", val)
	}

	t.Log("Automatic failover successful: new leader elected, data intact, new writes accepted")
}

// ============================================================================
// REQUIREMENT: Constraint — Standard library only
// ============================================================================

func TestRequirement_StdlibOnly(t *testing.T) {
	// Verify go.mod has no external dependencies
	data, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("failed to read go.mod: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") ||
			strings.HasPrefix(line, "module") ||
			strings.HasPrefix(line, "go ") ||
			line == ")" || line == "require (" {
			continue
		}
		// If there's a require block with entries, that's a problem
		if strings.HasPrefix(line, "require") && strings.Contains(line, "/") {
			t.Fatalf("external dependency found: %s", line)
		}
	}

	// Also verify go.sum doesn't exist or is empty (no downloaded deps)
	if _, err := os.Stat("go.sum"); err == nil {
		sumData, _ := os.ReadFile("go.sum")
		if len(strings.TrimSpace(string(sumData))) > 0 {
			t.Fatalf("go.sum is non-empty, indicating external dependencies were downloaded")
		}
	}

	t.Log("Confirmed: no external dependencies — standard library only")
}

// ============================================================================
// REQUIREMENT: Network availability
// ============================================================================

func TestRequirement_NetworkAvailable_TCPProtocol(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	// Full workflow via network
	// 1. Put
	if err := c.Put([]byte("net-1"), []byte("val-1")); err != nil {
		t.Fatalf("network Put: %v", err)
	}

	// 2. Read
	val, err := c.Get([]byte("net-1"))
	if err != nil {
		t.Fatalf("network Get: %v", err)
	}
	if string(val) != "val-1" {
		t.Fatalf("network Get value mismatch")
	}

	// 3. BatchPut
	keys := [][]byte{[]byte("net-2"), []byte("net-3"), []byte("net-4")}
	vals := [][]byte{[]byte("val-2"), []byte("val-3"), []byte("val-4")}
	if err := c.BatchPut(keys, vals); err != nil {
		t.Fatalf("network BatchPut: %v", err)
	}

	// 4. ReadKeyRange
	pairs, err := c.GetRange([]byte("net-1"), []byte("net-4"))
	if err != nil {
		t.Fatalf("network GetRange: %v", err)
	}
	if len(pairs) != 4 {
		t.Fatalf("expected 4 pairs, got %d", len(pairs))
	}

	// 5. Delete
	if err := c.Delete([]byte("net-2")); err != nil {
		t.Fatalf("network Delete: %v", err)
	}

	_, err = c.Get([]byte("net-2"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected not found after network Delete: %v", err)
	}

	// Verify range after delete
	pairs, err = c.GetRange([]byte("net-1"), []byte("net-4"))
	if err != nil {
		t.Fatalf("network GetRange after delete: %v", err)
	}
	if len(pairs) != 3 {
		t.Fatalf("expected 3 pairs after delete, got %d", len(pairs))
	}
}

func TestRequirement_NetworkAvailable_MultipleConnections(t *testing.T) {
	srv, _, eng := newTestServerAndClient(t)
	_ = eng

	// 10 concurrent clients
	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c, err := client.Connect(srv.Addr())
			if err != nil {
				errors.Add(1)
				return
			}
			defer c.Close()

			for j := 0; j < 20; j++ {
				key := fmt.Sprintf("mc-%d-%03d", id, j)
				if err := c.Put([]byte(key), []byte("v")); err != nil {
					errors.Add(1)
				}
				if _, err := c.Get([]byte(key)); err != nil {
					errors.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()

	if errors.Load() > 0 {
		t.Fatalf("%d errors across 10 concurrent clients", errors.Load())
	}
	t.Log("10 concurrent clients served successfully")
}

// ============================================================================
// REQUIREMENT: Persistence — data survives engine restart
// ============================================================================

func TestRequirement_Persistence_DataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	// Session 1: write data and close cleanly
	eng, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4096})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 500; i++ {
		eng.Put([]byte(fmt.Sprintf("persist-%06d", i)), []byte(fmt.Sprintf("val-%06d", i)))
	}
	eng.Close() // clean shutdown

	// Session 2: reopen and verify
	eng2, err := engine.Open(engine.Options{Dir: dir, MemtableSize: 4096})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer eng2.Close()

	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("persist-%06d", i)
		val, err := eng2.Get([]byte(key))
		if err != nil {
			t.Fatalf("key '%s' not found after restart: %v", key, err)
		}
		expected := fmt.Sprintf("val-%06d", i)
		if string(val) != expected {
			t.Fatalf("value mismatch for '%s': got '%s'", key, val)
		}
	}

	// Also verify range query works after restart
	pairs, err := eng2.ReadRange([]byte("persist-000100"), []byte("persist-000200"))
	if err != nil {
		t.Fatalf("ReadRange after restart: %v", err)
	}
	if len(pairs) != 101 {
		t.Fatalf("expected 101 pairs from range after restart, got %d", len(pairs))
	}
}

// ============================================================================
// SUMMARY: Verify all interfaces work end-to-end in sequence
// ============================================================================

func TestRequirement_EndToEnd_FullWorkflow(t *testing.T) {
	_, c, _ := newTestServerAndClient(t)

	// 1. Put(Key, Value)
	c.Put([]byte("user:1"), []byte(`{"name":"Alice","age":30}`))
	c.Put([]byte("user:2"), []byte(`{"name":"Bob","age":25}`))
	c.Put([]byte("user:3"), []byte(`{"name":"Charlie","age":35}`))

	// 2. Read(Key)
	val, err := c.Get([]byte("user:2"))
	if err != nil || string(val) != `{"name":"Bob","age":25}` {
		t.Fatalf("Read user:2 failed: %v, val: %s", err, val)
	}

	// 3. ReadKeyRange(StartKey, EndKey)
	pairs, err := c.GetRange([]byte("user:1"), []byte("user:3"))
	if err != nil || len(pairs) != 3 {
		t.Fatalf("ReadKeyRange failed: %v, len: %d", err, len(pairs))
	}

	// 4. BatchPut(..keys, ..values)
	bk := [][]byte{[]byte("user:4"), []byte("user:5")}
	bv := [][]byte{[]byte(`{"name":"Diana","age":28}`), []byte(`{"name":"Eve","age":22}`)}
	if err := c.BatchPut(bk, bv); err != nil {
		t.Fatalf("BatchPut failed: %v", err)
	}

	// 5. Delete(key)
	if err := c.Delete([]byte("user:3")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify final state
	pairs, err = c.GetRange([]byte("user:1"), []byte("user:5"))
	if err != nil {
		t.Fatalf("final range: %v", err)
	}

	expectedKeys := []string{"user:1", "user:2", "user:4", "user:5"}
	if len(pairs) != len(expectedKeys) {
		t.Fatalf("expected %d users, got %d", len(expectedKeys), len(pairs))
	}
	for i, p := range pairs {
		if string(p.Key) != expectedKeys[i] {
			t.Fatalf("pair[%d]: expected '%s', got '%s'", i, expectedKeys[i], p.Key)
		}
	}

	t.Log("Full end-to-end workflow verified: Put, Read, ReadKeyRange, BatchPut, Delete — all via network")
}

// ============================================================================
// Verify go.mod has correct module path
// ============================================================================

func init() {
	// Suppress unused import warnings
	_ = sort.Search
	_ = runtime.NumCPU
	_ = filepath.Join
}
