package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestSkipListPutGet(t *testing.T) {
	sl := NewSkipList()
	keys := make([][]byte, 100)
	values := make([][]byte, 100)

	for i := 0; i < 100; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%04d", rand.Intn(10000)))
		values[i] = []byte(fmt.Sprintf("value-%04d", i))
		sl.Put(keys[i], values[i])
	}

	for i := 0; i < 100; i++ {
		val, found := sl.Get(keys[i])
		if !found {
			t.Fatalf("expected key %q to be found", keys[i])
		}
		// The value may have been overwritten if there were duplicate random keys,
		// so just verify it was found and is non-nil.
		if val == nil {
			t.Fatalf("expected non-nil value for key %q", keys[i])
		}
	}

	// Verify a missing key returns not found.
	_, found := sl.Get([]byte("nonexistent-key"))
	if found {
		t.Fatal("expected nonexistent key to not be found")
	}
}

func TestSkipListOverwrite(t *testing.T) {
	sl := NewSkipList()

	key := []byte("mykey")
	sl.Put(key, []byte("first"))
	sl.Put(key, []byte("second"))

	val, found := sl.Get(key)
	if !found {
		t.Fatal("expected key to be found")
	}
	if !bytes.Equal(val, []byte("second")) {
		t.Fatalf("expected value %q, got %q", "second", val)
	}

	// Count should be 1 since it was an overwrite, not a second insert.
	if sl.Len() != 1 {
		t.Fatalf("expected Len()=1, got %d", sl.Len())
	}
}

func TestSkipListDelete(t *testing.T) {
	sl := NewSkipList()

	key := []byte("delme")
	sl.Put(key, []byte("alive"))

	val, found := sl.Get(key)
	if !found || !bytes.Equal(val, []byte("alive")) {
		t.Fatal("expected key to exist with value 'alive'")
	}

	sl.Delete(key)

	val, found = sl.Get(key)
	if !found {
		t.Fatal("expected tombstone key to be found")
	}
	if val != nil {
		t.Fatalf("expected nil value for tombstone, got %q", val)
	}
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()

	keys := []string{"banana", "apple", "cherry", "date", "elderberry"}
	for _, k := range keys {
		sl.Put([]byte(k), []byte("v-"+k))
	}

	sort.Strings(keys)

	it := sl.NewIterator()
	it.SeekToFirst()

	var collected []string
	for it.Valid() {
		collected = append(collected, string(it.Key()))
		it.Next()
	}

	if len(collected) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(collected))
	}
	for i, k := range keys {
		if collected[i] != k {
			t.Fatalf("at index %d: expected %q, got %q", i, k, collected[i])
		}
	}
}

func TestSkipListIteratorSeek(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i*10))
		sl.Put(key, []byte(fmt.Sprintf("val-%02d", i)))
	}

	it := sl.NewIterator()

	// Seek to an exact key.
	it.Seek([]byte("key-30"))
	if !it.Valid() {
		t.Fatal("expected iterator to be valid after Seek")
	}
	if string(it.Key()) != "key-30" {
		t.Fatalf("expected key-30, got %s", it.Key())
	}

	// Seek to a key between existing keys: should land on the next one.
	it.Seek([]byte("key-25"))
	if !it.Valid() {
		t.Fatal("expected iterator to be valid after Seek")
	}
	if string(it.Key()) != "key-30" {
		t.Fatalf("expected key-30, got %s", it.Key())
	}

	// Seek past the last key.
	it.Seek([]byte("key-99"))
	if it.Valid() {
		t.Fatal("expected iterator to be invalid after seeking past end")
	}

	// Seek before the first key.
	it.Seek([]byte("key-00"))
	if !it.Valid() {
		t.Fatal("expected iterator to be valid")
	}
	if string(it.Key()) != "key-00" {
		t.Fatalf("expected key-00, got %s", it.Key())
	}

	// Verify IsTombstone on a non-tombstone entry.
	if it.IsTombstone() {
		t.Fatal("expected IsTombstone to be false for a non-deleted key")
	}
}

func TestMemtablePutGet(t *testing.T) {
	mt := NewMemtable(0) // default size

	if err := mt.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, found, tombstone := mt.Get([]byte("hello"))
	if !found {
		t.Fatal("expected key to be found")
	}
	if tombstone {
		t.Fatal("expected key to not be a tombstone")
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("expected 'world', got %q", val)
	}

	// Delete and verify tombstone.
	if err := mt.Delete([]byte("hello")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val, found, tombstone = mt.Get([]byte("hello"))
	if !found {
		t.Fatal("expected deleted key to be found (tombstone)")
	}
	if !tombstone {
		t.Fatal("expected tombstone=true")
	}
	if val != nil {
		t.Fatalf("expected nil value for tombstone, got %q", val)
	}
}

func TestMemtableFreeze(t *testing.T) {
	mt := NewMemtable(0)

	if err := mt.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mt.IsFrozen() {
		t.Fatal("expected memtable to not be frozen initially")
	}

	mt.Freeze()

	if !mt.IsFrozen() {
		t.Fatal("expected memtable to be frozen after Freeze()")
	}

	err := mt.Put([]byte("key2"), []byte("val2"))
	if err != ErrFrozen {
		t.Fatalf("expected ErrFrozen, got %v", err)
	}

	err = mt.Delete([]byte("key1"))
	if err != ErrFrozen {
		t.Fatalf("expected ErrFrozen on delete, got %v", err)
	}

	// Reads should still work on a frozen memtable.
	val, found, _ := mt.Get([]byte("key1"))
	if !found || !bytes.Equal(val, []byte("val1")) {
		t.Fatal("expected to read from frozen memtable")
	}
}

func TestMemtableShouldFlush(t *testing.T) {
	maxSize := 256 // small threshold for testing
	mt := NewMemtable(maxSize)

	if mt.ShouldFlush() {
		t.Fatal("should not need flush when empty")
	}

	// Insert data until we exceed the threshold.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d-padding-to-increase-size", i))
		if err := mt.Put(key, val); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if !mt.ShouldFlush() {
		t.Fatalf("expected ShouldFlush()=true, approximate size=%d, maxSize=%d",
			mt.ApproximateSize(), maxSize)
	}
}

func TestMemtableConcurrency(t *testing.T) {
	mt := NewMemtable(0)
	var wg sync.WaitGroup

	// Launch 10 goroutines doing concurrent puts and gets.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := []byte(fmt.Sprintf("goroutine-%d-key-%d", id, i))
				val := []byte(fmt.Sprintf("val-%d-%d", id, i))

				if err := mt.Put(key, val); err != nil {
					t.Errorf("goroutine %d: put error: %v", id, err)
					return
				}

				// Read back the key we just wrote.
				v, found, _ := mt.Get(key)
				if !found {
					t.Errorf("goroutine %d: key %q not found", id, key)
					return
				}
				if !bytes.Equal(v, val) {
					// Another goroutine may have overwritten if keys collide,
					// but with unique keys per goroutine this should not happen.
					t.Errorf("goroutine %d: expected %q, got %q", id, val, v)
					return
				}
			}
		}(g)
	}

	wg.Wait()
}
