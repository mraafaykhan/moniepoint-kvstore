package memtable

import (
	"bytes"
	"math/rand"
	"time"
)

const (
	maxLevel = 20
	pFactor  = 0.25
)

var rng *rand.Rand

func init() {
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// node represents a single entry in the skip list.
type node struct {
	key     []byte
	value   []byte // nil means tombstone (deleted)
	forward []*node
}

// SkipList is a probabilistic sorted data structure that supports
// O(log n) average insertion, deletion, and lookup.
type SkipList struct {
	head  *node
	level int
	count int
	size  int // approximate memory usage in bytes
}

// NewSkipList creates and returns a new empty SkipList.
func NewSkipList() *SkipList {
	head := &node{
		forward: make([]*node, maxLevel),
	}
	return &SkipList{
		head:  head,
		level: 0,
		count: 0,
		size:  0,
	}
}

// randomLevel generates a random level for a new node.
func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rng.Float64() < pFactor {
		lvl++
	}
	return lvl
}

// Put inserts or updates a key-value pair in the skip list.
// A nil value represents a tombstone (deletion marker).
func (sl *SkipList) Put(key, value []byte) {
	update := make([]*node, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Check if key already exists.
	target := current.forward[0]
	if target != nil && bytes.Equal(target.key, key) {
		// Update existing node. Adjust size for value change.
		oldValSize := len(target.value)
		newValSize := len(value)
		sl.size += newValSize - oldValSize
		target.value = copyBytes(value)
		return
	}

	// Insert new node.
	lvl := randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &node{
		key:     copyBytes(key),
		value:   copyBytes(value),
		forward: make([]*node, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.count++
	// Approximate size: key + value + forward slice overhead.
	sl.size += len(key) + len(value) + lvl*8 + 64
}

// Get looks up a key and returns its value and whether it was found.
// A tombstone returns (nil, true).
func (sl *SkipList) Get(key []byte) (value []byte, found bool) {
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	target := current.forward[0]
	if target != nil && bytes.Equal(target.key, key) {
		return target.value, true
	}
	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone (nil value).
func (sl *SkipList) Delete(key []byte) {
	sl.Put(key, nil)
}

// Len returns the number of entries in the skip list.
func (sl *SkipList) Len() int {
	return sl.count
}

// Size returns the approximate memory usage in bytes.
func (sl *SkipList) Size() int {
	return sl.size
}

// NewIterator returns a new iterator positioned before the first element.
func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{
		list:    sl,
		current: nil,
	}
}

// copyBytes returns a copy of the given byte slice, or nil if the input is nil.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp
}

// Iterator allows sequential traversal of the skip list in sorted order.
type Iterator struct {
	list    *SkipList
	current *node
}

// SeekToFirst positions the iterator at the first key.
func (it *Iterator) SeekToFirst() {
	it.current = it.list.head.forward[0]
}

// Seek positions the iterator at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	current := it.list.head
	for i := it.list.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, target) < 0 {
			current = current.forward[i]
		}
	}
	it.current = current.forward[0]
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}

// Valid reports whether the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.current != nil
}

// Key returns the key at the current iterator position.
func (it *Iterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// Value returns the value at the current iterator position.
// A nil value indicates a tombstone.
func (it *Iterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.value
}

// IsTombstone reports whether the current entry is a deletion marker.
func (it *Iterator) IsTombstone() bool {
	if it.current == nil {
		return false
	}
	return it.current.value == nil
}
