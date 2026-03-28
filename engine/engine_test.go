package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "engine-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func testOpts(dir string) Options {
	return Options{
		Dir:              dir,
		MemtableSize:     1024, // 1KB for quick flushes
		L0CompactTrigger: 4,
		MaxLevels:        7,
		BlockSize:        256,
	}
}

func TestEngineBasicPutGet(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		expected := []byte(fmt.Sprintf("val-%03d", i))
		got, err := eng.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("key-%03d: got %q, want %q", i, got, expected)
		}
	}
}

func TestEngineDelete(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	key := []byte("mykey")
	val := []byte("myval")

	if err := eng.Put(key, val); err != nil {
		t.Fatal(err)
	}
	got, err := eng.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("got %q, want %q", got, val)
	}

	if err := eng.Delete(key); err != nil {
		t.Fatal(err)
	}
	_, err = eng.Get(key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestEngineBatchPut(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	keys := make([][]byte, 50)
	vals := make([][]byte, 50)
	for i := 0; i < 50; i++ {
		keys[i] = []byte(fmt.Sprintf("batch-key-%03d", i))
		vals[i] = []byte(fmt.Sprintf("batch-val-%03d", i))
	}

	if err := eng.BatchPut(keys, vals); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		got, err := eng.Get(keys[i])
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, vals[i]) {
			t.Fatalf("key %d: got %q, want %q", i, got, vals[i])
		}
	}
}

func TestEngineReadRange(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put keys "a" through "z".
	for c := byte('a'); c <= 'z'; c++ {
		key := []byte{c}
		val := []byte(fmt.Sprintf("val-%c", c))
		if err := eng.Put(key, val); err != nil {
			t.Fatal(err)
		}
	}

	results, err := eng.ReadRange([]byte("d"), []byte("h"))
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte{'d', 'e', 'f', 'g', 'h'}
	if len(results) != len(expected) {
		t.Fatalf("got %d results, want %d", len(results), len(expected))
	}
	for i, kv := range results {
		if !bytes.Equal(kv.Key, []byte{expected[i]}) {
			t.Fatalf("result %d: got key %q, want %q", i, kv.Key, []byte{expected[i]})
		}
	}
}

func TestEngineOverwrite(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	key := []byte("overwrite-key")
	if err := eng.Put(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put(key, []byte("second")); err != nil {
		t.Fatal(err)
	}

	got, err := eng.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("second")) {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

func TestEngineFlush(t *testing.T) {
	dir := tempDir(t)
	opts := testOpts(dir)
	opts.MemtableSize = 512 // very small to trigger flushes
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Write enough data to trigger multiple flushes.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("flush-key-%05d", i))
		val := []byte(fmt.Sprintf("flush-val-%05d-padding-to-make-it-bigger", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Wait briefly for background flushes to complete.
	time.Sleep(200 * time.Millisecond)

	// Verify all data is readable.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("flush-key-%05d", i))
		expected := []byte(fmt.Sprintf("flush-val-%05d-padding-to-make-it-bigger", i))
		got, err := eng.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: got %q, want %q", i, got, expected)
		}
	}

	// Verify at least one SSTable file exists.
	matches, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(matches) == 0 {
		t.Fatal("expected SSTable files to exist after flush")
	}
}

func TestEngineCrashRecovery(t *testing.T) {
	dir := tempDir(t)
	opts := testOpts(dir)
	opts.MemtableSize = 1024 * 1024 // large enough to NOT flush

	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Write some data.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("crash-key-%03d", i))
		val := []byte(fmt.Sprintf("crash-val-%03d", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate crash: close WAL directly without calling engine.Close().
	// This leaves data only in the WAL, not flushed.
	eng.wal.Close()
	eng.closeReaders()
	eng.manifest.Close()
	close(eng.closeCh)
	eng.wg.Wait()

	// Reopen the engine.
	eng2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Close()

	// Verify data recovered from WAL.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("crash-key-%03d", i))
		expected := []byte(fmt.Sprintf("crash-val-%03d", i))
		got, err := eng2.Get(key)
		if err != nil {
			t.Fatalf("get %d after recovery: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestEngineCompaction(t *testing.T) {
	dir := tempDir(t)
	opts := testOpts(dir)
	opts.MemtableSize = 256
	opts.L0CompactTrigger = 4

	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Write enough data to trigger multiple flushes and compaction.
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("compact-key-%05d", i))
		val := []byte(fmt.Sprintf("compact-val-%05d-extra-padding", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Wait for background flush and compaction.
	time.Sleep(500 * time.Millisecond)

	// Verify all data still readable.
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("compact-key-%05d", i))
		expected := []byte(fmt.Sprintf("compact-val-%05d-extra-padding", i))
		got, err := eng.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: got %q, want %q", i, got, expected)
		}
	}
}

func TestEngineLargeDataset(t *testing.T) {
	dir := tempDir(t)
	opts := testOpts(dir)
	opts.MemtableSize = 4096 // 4KB

	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	n := 10000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("large-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		if err := eng.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Wait for flushes.
	time.Sleep(500 * time.Millisecond)

	// Verify all via Get.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("large-%06d", i))
		expected := []byte(fmt.Sprintf("value-%06d", i))
		got, err := eng.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !bytes.Equal(got, expected) {
			t.Fatalf("key %d: got %q, want %q", i, got, expected)
		}
	}

	// Verify subset via ReadRange.
	results, err := eng.ReadRange([]byte("large-005000"), []byte("large-005009"))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Fatalf("range: got %d results, want 10", len(results))
	}
	for i, kv := range results {
		expected := []byte(fmt.Sprintf("large-%06d", 5000+i))
		if !bytes.Equal(kv.Key, expected) {
			t.Fatalf("range result %d: got %q, want %q", i, kv.Key, expected)
		}
	}
}

func TestEngineTombstoneInRange(t *testing.T) {
	dir := tempDir(t)
	eng, err := Open(testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Put keys "a" through "j".
	for c := byte('a'); c <= 'j'; c++ {
		if err := eng.Put([]byte{c}, []byte{c, c}); err != nil {
			t.Fatal(err)
		}
	}

	// Delete "c", "f", "h".
	for _, c := range []byte{'c', 'f', 'h'} {
		if err := eng.Delete([]byte{c}); err != nil {
			t.Fatal(err)
		}
	}

	results, err := eng.ReadRange([]byte("a"), []byte("j"))
	if err != nil {
		t.Fatal(err)
	}

	expectedKeys := []byte{'a', 'b', 'd', 'e', 'g', 'i', 'j'}
	if len(results) != len(expectedKeys) {
		var gotKeys []string
		for _, kv := range results {
			gotKeys = append(gotKeys, string(kv.Key))
		}
		t.Fatalf("got %d results %v, want %d", len(results), gotKeys, len(expectedKeys))
	}

	// Sort results to be safe (they should already be sorted).
	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].Key, results[j].Key) < 0
	})

	for i, kv := range results {
		if !bytes.Equal(kv.Key, []byte{expectedKeys[i]}) {
			t.Fatalf("result %d: got key %q, want %q", i, kv.Key, []byte{expectedKeys[i]})
		}
	}
}
