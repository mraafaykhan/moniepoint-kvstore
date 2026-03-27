package memtable

import (
	"errors"
	"sync"
)

const defaultMaxSize = 4 * 1024 * 1024 // 4 MB

var ErrFrozen = errors.New("memtable is frozen")

// Memtable is a thread-safe wrapper around a SkipList that supports
// freezing for flush and a configurable size threshold.
type Memtable struct {
	mu      sync.RWMutex
	sl      *SkipList
	maxSize int
	frozen  bool
}

// NewMemtable creates a new Memtable with the given maximum size in bytes.
// If maxSize <= 0, the default of 4 MB is used.
func NewMemtable(maxSize int) *Memtable {
	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}
	return &Memtable{
		sl:      NewSkipList(),
		maxSize: maxSize,
	}
}

// Put inserts or updates a key-value pair. Returns ErrFrozen if the memtable
// has been frozen for flushing.
func (m *Memtable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrFrozen
	}

	m.sl.Put(key, value)
	return nil
}

// Get retrieves a value by key. It returns the value, whether the key was found,
// and whether the entry is a tombstone (deleted).
func (m *Memtable) Get(key []byte) (value []byte, found bool, tombstone bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.sl.Get(key)
	if !ok {
		return nil, false, false
	}
	return val, true, val == nil
}

// Delete marks a key as deleted by inserting a tombstone. Returns ErrFrozen
// if the memtable has been frozen.
func (m *Memtable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrFrozen
	}

	m.sl.Delete(key)
	return nil
}

// ShouldFlush reports whether the memtable has reached or exceeded its size threshold.
func (m *Memtable) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sl.Size() >= m.maxSize
}

// Freeze marks the memtable as immutable. After freezing, Put and Delete
// operations will return ErrFrozen.
func (m *Memtable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frozen = true
}

// IsFrozen reports whether the memtable is frozen.
func (m *Memtable) IsFrozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.frozen
}

// NewIterator returns a skip list iterator. The caller should either hold
// the appropriate lock or use this on a frozen memtable to ensure safety.
func (m *Memtable) NewIterator() *Iterator {
	return m.sl.NewIterator()
}

// ApproximateSize returns the approximate memory usage of the memtable in bytes.
func (m *Memtable) ApproximateSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sl.Size()
}
