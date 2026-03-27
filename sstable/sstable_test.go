package sstable

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func tempPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 10)

	// Add 1000 keys.
	for i := 0; i < 1000; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%06d", i)))
	}

	// All added keys must return true.
	for i := 0; i < 1000; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%06d", i))) {
			t.Fatalf("MayContain returned false for added key %d", i)
		}
	}

	// Check false positive rate on non-existent keys.
	falsePositives := 0
	total := 10000
	for i := 0; i < total; i++ {
		k := []byte(fmt.Sprintf("nonexistent-%06d", i))
		if bf.MayContain(k) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(total)
	t.Logf("Bloom filter false positive rate: %.4f (%d/%d)", rate, falsePositives, total)
	if rate > 0.05 {
		t.Errorf("false positive rate too high: %.4f", rate)
	}
}

func TestBloomFilterEncodeDecode(t *testing.T) {
	bf := NewBloomFilter(500, 10)
	for i := 0; i < 500; i++ {
		bf.Add([]byte(fmt.Sprintf("k-%05d", i)))
	}

	encoded := bf.Encode()
	bf2, err := DecodeBloomFilter(encoded)
	if err != nil {
		t.Fatal(err)
	}

	// All keys should still be found.
	for i := 0; i < 500; i++ {
		k := []byte(fmt.Sprintf("k-%05d", i))
		if !bf2.MayContain(k) {
			t.Fatalf("decoded bloom filter missing key %d", i)
		}
	}

	// Verify structural equality.
	if bf.m != bf2.m || bf.k != bf2.k {
		t.Fatalf("m/k mismatch: (%d,%d) vs (%d,%d)", bf.m, bf.k, bf2.m, bf2.k)
	}
	if !bytes.Equal(bf.bits, bf2.bits) {
		t.Fatal("bits mismatch after decode")
	}
}

func TestWriterReader(t *testing.T) {
	path := tempPath(t, "test.sst")

	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}

	n := 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		if err := w.Add(key, val, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if w.EntryCount() != n {
		t.Fatalf("entry count: got %d want %d", w.EntryCount(), n)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, found, err := r.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key %d not found", i)
		}
		expected := []byte(fmt.Sprintf("value-%06d", i))
		if !bytes.Equal(val, expected) {
			t.Fatalf("key %d: got %q want %q", i, val, expected)
		}
	}
}

func TestReaderMissing(t *testing.T) {
	path := tempPath(t, "test.sst")

	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := w.Add(key, val, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Keys that should not exist.
	missing := []string{"aaa", "zzz", "key-999999", "nokey"}
	for _, k := range missing {
		_, found, err := r.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("key %q should not be found", k)
		}
	}
}

func TestTombstones(t *testing.T) {
	path := tempPath(t, "test.sst")

	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		tombstone := i%3 == 0
		if tombstone {
			if err := w.Add(key, nil, true); err != nil {
				t.Fatal(err)
			}
		} else {
			val := []byte(fmt.Sprintf("val-%06d", i))
			if err := w.Add(key, val, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, found, err := r.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key %d not found", i)
		}
		if i%3 == 0 {
			if val != nil {
				t.Fatalf("key %d: tombstone should have nil value, got %q", i, val)
			}
		} else {
			expected := []byte(fmt.Sprintf("val-%06d", i))
			if !bytes.Equal(val, expected) {
				t.Fatalf("key %d: got %q want %q", i, val, expected)
			}
		}
	}
}

func TestIteratorFullScan(t *testing.T) {
	path := tempPath(t, "test.sst")

	w, err := NewWriter(path, 512) // small blocks for more coverage
	if err != nil {
		t.Fatal(err)
	}
	n := 500
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("val-%06d", i))
		if err := w.Add(key, val, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()

	count := 0
	var prevKey []byte
	for it.Valid() {
		k := it.Key()
		if prevKey != nil && bytes.Compare(prevKey, k) >= 0 {
			t.Fatalf("keys not in sorted order: %q >= %q", prevKey, k)
		}
		expected := []byte(fmt.Sprintf("key-%06d", count))
		if !bytes.Equal(k, expected) {
			t.Fatalf("entry %d: got key %q want %q", count, k, expected)
		}
		prevKey = append(prevKey[:0], k...)
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("iterator yielded %d entries, want %d", count, n)
	}
}

func TestIteratorSeek(t *testing.T) {
	path := tempPath(t, "test.sst")

	w, err := NewWriter(path, 256)
	if err != nil {
		t.Fatal(err)
	}
	// Write keys: key-000, key-010, key-020, ..., key-990
	var keys []string
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i*10)
		keys = append(keys, k)
		if err := w.Add([]byte(k), []byte(fmt.Sprintf("v%d", i)), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()

	// Seek to exact key.
	it.Seek([]byte("key-050"))
	if !it.Valid() {
		t.Fatal("seek to key-050: not valid")
	}
	if string(it.Key()) != "key-050" {
		t.Fatalf("seek to key-050: got %q", it.Key())
	}

	// Seek to key between two entries.
	it.Seek([]byte("key-055"))
	if !it.Valid() {
		t.Fatal("seek to key-055: not valid")
	}
	if string(it.Key()) != "key-060" {
		t.Fatalf("seek to key-055: got %q, want key-060", it.Key())
	}

	// Seek past all keys.
	it.Seek([]byte("zzz"))
	if it.Valid() {
		t.Fatalf("seek to zzz: should be invalid, got %q", it.Key())
	}

	// Seek before all keys.
	it.Seek([]byte("aaa"))
	if !it.Valid() {
		t.Fatal("seek to aaa: not valid")
	}
	if string(it.Key()) != "key-000" {
		t.Fatalf("seek to aaa: got %q, want key-000", it.Key())
	}
}

func TestLargeSSTable(t *testing.T) {
	path := tempPath(t, "large.sst")

	n := 100000
	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%08d", i))
		val := []byte(fmt.Sprintf("v-%08d", i))
		if err := w.Add(key, val, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Spot-check random keys.
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		idx := rng.Intn(n)
		key := []byte(fmt.Sprintf("k-%08d", idx))
		val, found, err := r.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key %d not found", idx)
		}
		expected := []byte(fmt.Sprintf("v-%08d", idx))
		if !bytes.Equal(val, expected) {
			t.Fatalf("key %d: got %q want %q", idx, val, expected)
		}
	}

	// Verify iterator yields all entries in order.
	it := r.NewIterator()
	it.SeekToFirst()
	count := 0
	var prevKey []byte
	for it.Valid() {
		k := it.Key()
		if prevKey != nil && bytes.Compare(prevKey, k) >= 0 {
			t.Fatalf("keys not sorted at entry %d", count)
		}
		prevKey = append(prevKey[:0], k...)
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("iterator yielded %d entries, want %d", count, n)
	}
}

func TestEmptySSTable(t *testing.T) {
	path := tempPath(t, "empty.sst")

	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if w.EntryCount() != 0 {
		t.Fatalf("entry count: got %d want 0", w.EntryCount())
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, found, err := r.Get([]byte("anything"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("empty sstable should not find any key")
	}

	it := r.NewIterator()
	it.SeekToFirst()
	if it.Valid() {
		t.Fatal("iterator on empty sstable should be invalid")
	}
}

// TestIteratorTombstones verifies the iterator correctly reports tombstone entries.
func TestIteratorTombstones(t *testing.T) {
	path := tempPath(t, "tombstone_iter.sst")

	w, err := NewWriter(path, 256)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if i%4 == 0 {
			if err := w.Add(key, nil, true); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := w.Add(key, []byte(fmt.Sprintf("v%d", i)), false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()
	i := 0
	for it.Valid() {
		if i%4 == 0 {
			if !it.IsTombstone() {
				t.Fatalf("entry %d should be tombstone", i)
			}
			if it.Value() != nil {
				t.Fatalf("tombstone entry %d should have nil value", i)
			}
		} else {
			if it.IsTombstone() {
				t.Fatalf("entry %d should not be tombstone", i)
			}
		}
		i++
		it.Next()
	}
	if i != 20 {
		t.Fatalf("got %d entries, want 20", i)
	}
}

// TestMinMaxKey verifies that the reader reports correct min and max keys.
func TestMinMaxKey(t *testing.T) {
	path := tempPath(t, "minmax.sst")

	w, err := NewWriter(path, 256)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("key-%04d", i))
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Add([]byte(k), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if string(r.MinKey()) != keys[0] {
		t.Fatalf("MinKey: got %q want %q", r.MinKey(), keys[0])
	}
	if string(r.MaxKey()) != keys[len(keys)-1] {
		t.Fatalf("MaxKey: got %q want %q", r.MaxKey(), keys[len(keys)-1])
	}
}

// TestFileCleanup ensures the file is actually closed after reader.Close().
func TestFileCleanup(t *testing.T) {
	path := tempPath(t, "cleanup.sst")

	w, err := NewWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Add([]byte("a"), []byte("b"), false); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// File should still exist on disk.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file should still exist: %v", err)
	}
}
