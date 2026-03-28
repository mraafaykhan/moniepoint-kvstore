package engine

import (
	"bytes"
	"container/heap"

	"github.com/raafay/kvstore/memtable"
	"github.com/raafay/kvstore/sstable"
)

// Iterator is the common interface for iterating over sorted key-value entries.
type Iterator interface {
	SeekToFirst()
	Seek(key []byte)
	Next()
	Valid() bool
	Key() []byte
	Value() []byte
	IsTombstone() bool
}

// ---------- memtable adapter ----------

type memtableIteratorAdapter struct {
	it *memtable.Iterator
}

func newMemtableIteratorAdapter(it *memtable.Iterator) *memtableIteratorAdapter {
	return &memtableIteratorAdapter{it: it}
}

func (a *memtableIteratorAdapter) SeekToFirst()       { a.it.SeekToFirst() }
func (a *memtableIteratorAdapter) Seek(key []byte)     { a.it.Seek(key) }
func (a *memtableIteratorAdapter) Next()               { a.it.Next() }
func (a *memtableIteratorAdapter) Valid() bool          { return a.it.Valid() }
func (a *memtableIteratorAdapter) Key() []byte          { return a.it.Key() }
func (a *memtableIteratorAdapter) Value() []byte        { return a.it.Value() }
func (a *memtableIteratorAdapter) IsTombstone() bool    { return a.it.IsTombstone() }

// ---------- sstable adapter ----------

type sstableIteratorAdapter struct {
	it *sstable.Iterator
}

func newSSTableIteratorAdapter(it *sstable.Iterator) *sstableIteratorAdapter {
	return &sstableIteratorAdapter{it: it}
}

func (a *sstableIteratorAdapter) SeekToFirst()       { a.it.SeekToFirst() }
func (a *sstableIteratorAdapter) Seek(key []byte)     { a.it.Seek(key) }
func (a *sstableIteratorAdapter) Next()               { a.it.Next() }
func (a *sstableIteratorAdapter) Valid() bool          { return a.it.Valid() }
func (a *sstableIteratorAdapter) Key() []byte          { return a.it.Key() }
func (a *sstableIteratorAdapter) Value() []byte        { return a.it.Value() }
func (a *sstableIteratorAdapter) IsTombstone() bool    { return a.it.IsTombstone() }

// ---------- merge iterator ----------

// heapItem holds an iterator with its priority (lower = newer data).
type heapItem struct {
	iter     Iterator
	priority int
}

type mergeHeap []heapItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].iter.Key(), h[j].iter.Key())
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].priority < h[j].priority
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// MergeIterator merges multiple sorted iterators into a single sorted stream.
// When duplicate keys appear, the iterator with the lowest priority (newest) wins.
type MergeIterator struct {
	iters []heapItem
	h     mergeHeap
	cur   *heapItem // current winning item cached out of heap for Key/Value access
}

// NewMergeIterator creates a merge iterator. Each element of iters is paired
// with a priority; lower priority means newer data.
func NewMergeIterator(iters []Iterator, priorities []int) *MergeIterator {
	items := make([]heapItem, len(iters))
	for i := range iters {
		items[i] = heapItem{iter: iters[i], priority: priorities[i]}
	}
	return &MergeIterator{iters: items}
}

func (m *MergeIterator) buildHeap() {
	m.h = m.h[:0]
	for _, item := range m.iters {
		if item.iter.Valid() {
			m.h = append(m.h, item)
		}
	}
	heap.Init(&m.h)
}

func (m *MergeIterator) advance() {
	m.cur = nil
	if len(m.h) == 0 {
		return
	}

	// Pop the winner (smallest key, lowest priority).
	winner := heap.Pop(&m.h).(heapItem)
	m.cur = &winner
	winKey := winner.iter.Key()

	// Skip all other iterators that have the same key (dedup).
	for len(m.h) > 0 && bytes.Equal(m.h[0].iter.Key(), winKey) {
		dup := heap.Pop(&m.h).(heapItem)
		dup.iter.Next()
		if dup.iter.Valid() {
			heap.Push(&m.h, dup)
		}
	}
}

func (m *MergeIterator) SeekToFirst() {
	for _, item := range m.iters {
		item.iter.SeekToFirst()
	}
	m.buildHeap()
	m.advance()
}

func (m *MergeIterator) Seek(key []byte) {
	for _, item := range m.iters {
		item.iter.Seek(key)
	}
	m.buildHeap()
	m.advance()
}

func (m *MergeIterator) Next() {
	if m.cur == nil {
		return
	}
	// Advance the winner.
	m.cur.iter.Next()
	if m.cur.iter.Valid() {
		heap.Push(&m.h, *m.cur)
	}
	m.advance()
}

func (m *MergeIterator) Valid() bool {
	return m.cur != nil
}

func (m *MergeIterator) Key() []byte {
	if m.cur == nil {
		return nil
	}
	return m.cur.iter.Key()
}

func (m *MergeIterator) Value() []byte {
	if m.cur == nil {
		return nil
	}
	return m.cur.iter.Value()
}

func (m *MergeIterator) IsTombstone() bool {
	if m.cur == nil {
		return false
	}
	return m.cur.iter.IsTombstone()
}
