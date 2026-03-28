package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/raafay/kvstore/memtable"
	"github.com/raafay/kvstore/sstable"
	"github.com/raafay/kvstore/wal"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("key not found")

// KVPair holds a key-value pair for range query results.
type KVPair struct {
	Key   []byte
	Value []byte
}

// Options configures the LSM-Tree engine.
type Options struct {
	Dir              string
	MemtableSize     int // default 4MB
	L0CompactTrigger int // default 4
	MaxLevels        int // default 7
	BlockSize        int // SSTable block size, default 4096
}

func (o *Options) applyDefaults() {
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 * 1024 * 1024
	}
	if o.L0CompactTrigger <= 0 {
		o.L0CompactTrigger = 4
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = 7
	}
	if o.BlockSize <= 0 {
		o.BlockSize = 4096
	}
}

// flushTask describes a memtable that needs to be written to an SSTable.
type flushTask struct {
	mem   *memtable.Memtable
	walID uint64
}

// Engine is the core LSM-Tree storage engine.
type Engine struct {
	mu            sync.RWMutex
	dir           string
	opts          Options
	wal           *wal.WAL
	memtable      *memtable.Memtable
	immutableMems []*memtable.Memtable // newest first

	levels   [][]*sstable.Reader // levels[0] = L0, etc.
	manifest *Manifest

	flushCh   chan *flushTask
	compactCh chan struct{}
	closeCh   chan struct{}
	wg        sync.WaitGroup

	nextFileNum atomic.Uint64
	closed      bool
}

// Open creates or opens an LSM-Tree engine at the directory specified in opts.
func Open(opts Options) (*Engine, error) {
	opts.applyDefaults()

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: mkdir: %w", err)
	}

	manifest, err := OpenManifest(opts.Dir, opts.MaxLevels)
	if err != nil {
		return nil, fmt.Errorf("engine: open manifest: %w", err)
	}

	e := &Engine{
		dir:       opts.Dir,
		opts:      opts,
		manifest:  manifest,
		levels:    make([][]*sstable.Reader, opts.MaxLevels),
		flushCh:   make(chan *flushTask, 16),
		compactCh: make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
	}

	// Initialize nextFileNum from manifest.
	var maxFileNum uint64
	allLevels := manifest.GetAllLevels()
	for _, lvl := range allLevels {
		for _, meta := range lvl {
			if meta.FileNum > maxFileNum {
				maxFileNum = meta.FileNum
			}
		}
	}
	e.nextFileNum.Store(maxFileNum + 1)

	// Open all SSTable readers per manifest.
	for level, metas := range allLevels {
		for _, meta := range metas {
			path := e.sstPath(meta.FileNum)
			reader, err := sstable.OpenReader(path)
			if err != nil {
				e.closeReaders()
				manifest.Close()
				return nil, fmt.Errorf("engine: open sstable %d: %w", meta.FileNum, err)
			}
			e.levels[level] = append(e.levels[level], reader)
		}
	}

	// Open WAL.
	walDir := filepath.Join(opts.Dir, "wal")
	w, err := wal.Open(walDir, 0)
	if err != nil {
		e.closeReaders()
		manifest.Close()
		return nil, fmt.Errorf("engine: open wal: %w", err)
	}
	e.wal = w

	// Create memtable and replay WAL.
	e.memtable = memtable.NewMemtable(opts.MemtableSize)
	if err := w.Replay(func(rec wal.Record) error {
		switch rec.Type {
		case wal.RecordPut:
			return e.memtable.Put(rec.Key, rec.Value)
		case wal.RecordDelete:
			return e.memtable.Delete(rec.Key)
		}
		return nil
	}); err != nil {
		w.Close()
		e.closeReaders()
		manifest.Close()
		return nil, fmt.Errorf("engine: replay wal: %w", err)
	}

	// Start background goroutines.
	e.wg.Add(2)
	go e.flushLoop()
	go e.compactionLoop()

	return e, nil
}

func (e *Engine) allocFileNum() uint64 {
	return e.nextFileNum.Add(1) - 1
}

func (e *Engine) sstPath(fileNum uint64) string {
	return filepath.Join(e.dir, fmt.Sprintf("%06d.sst", fileNum))
}

// Put inserts a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(wal.Record{Type: wal.RecordPut, Key: key, Value: value}); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}
	if err := e.memtable.Put(key, value); err != nil {
		return fmt.Errorf("engine: memtable put: %w", err)
	}
	e.maybeScheduleFlush()
	return nil
}

// Delete marks a key as deleted.
func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(wal.Record{Type: wal.RecordDelete, Key: key}); err != nil {
		return fmt.Errorf("engine: wal append: %w", err)
	}
	if err := e.memtable.Delete(key); err != nil {
		return fmt.Errorf("engine: memtable delete: %w", err)
	}
	e.maybeScheduleFlush()
	return nil
}

// BatchPut inserts multiple key-value pairs atomically.
func (e *Engine) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("engine: keys and values length mismatch")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	recs := make([]wal.Record, len(keys))
	for i := range keys {
		if values[i] == nil {
			recs[i] = wal.Record{Type: wal.RecordDelete, Key: keys[i]}
		} else {
			recs[i] = wal.Record{Type: wal.RecordPut, Key: keys[i], Value: values[i]}
		}
	}
	if err := e.wal.AppendBatch(recs); err != nil {
		return fmt.Errorf("engine: wal batch: %w", err)
	}

	for i := range keys {
		if values[i] == nil {
			if err := e.memtable.Delete(keys[i]); err != nil {
				return fmt.Errorf("engine: memtable delete: %w", err)
			}
		} else {
			if err := e.memtable.Put(keys[i], values[i]); err != nil {
				return fmt.Errorf("engine: memtable put: %w", err)
			}
		}
	}
	e.maybeScheduleFlush()
	return nil
}

// Get retrieves the value for a key. Returns ErrNotFound if not present.
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// 1. Active memtable.
	if val, found, tomb := e.memtable.Get(key); found {
		if tomb {
			return nil, ErrNotFound
		}
		return val, nil
	}

	// 2. Immutable memtables (newest first).
	for _, imm := range e.immutableMems {
		if val, found, tomb := imm.Get(key); found {
			if tomb {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	// 3. L0 SSTables (all, newest first = reverse order in slice).
	for i := len(e.levels[0]) - 1; i >= 0; i-- {
		r := e.levels[0][i]
		if !r.MayContain(key) {
			continue
		}
		val, found, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("engine: sstable get: %w", err)
		}
		if found {
			if val == nil {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	// 4. L1+ (at most one SSTable per level).
	for level := 1; level < len(e.levels); level++ {
		r := e.findSSTableForKey(level, key)
		if r == nil {
			continue
		}
		if !r.MayContain(key) {
			continue
		}
		val, found, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("engine: sstable get: %w", err)
		}
		if found {
			if val == nil {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	return nil, ErrNotFound
}

// ReadRange returns all key-value pairs in [startKey, endKey] (inclusive),
// excluding tombstoned keys.
func (e *Engine) ReadRange(startKey, endKey []byte) ([]KVPair, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var iters []Iterator
	var priorities []int
	priority := 0

	// Active memtable.
	memIt := newMemtableIteratorAdapter(e.memtable.NewIterator())
	iters = append(iters, memIt)
	priorities = append(priorities, priority)
	priority++

	// Immutable memtables (newest first).
	for _, imm := range e.immutableMems {
		it := newMemtableIteratorAdapter(imm.NewIterator())
		iters = append(iters, it)
		priorities = append(priorities, priority)
		priority++
	}

	// L0 SSTables (newest first = reverse order).
	for i := len(e.levels[0]) - 1; i >= 0; i-- {
		it := newSSTableIteratorAdapter(e.levels[0][i].NewIterator())
		iters = append(iters, it)
		priorities = append(priorities, priority)
		priority++
	}

	// L1+ SSTables.
	for level := 1; level < len(e.levels); level++ {
		for _, r := range e.levels[level] {
			it := newSSTableIteratorAdapter(r.NewIterator())
			iters = append(iters, it)
			priorities = append(priorities, priority)
			priority++
		}
	}

	mi := NewMergeIterator(iters, priorities)
	mi.Seek(startKey)

	var results []KVPair
	for mi.Valid() {
		k := mi.Key()
		if bytes.Compare(k, endKey) > 0 {
			break
		}
		if !mi.IsTombstone() {
			results = append(results, KVPair{
				Key:   append([]byte(nil), k...),
				Value: append([]byte(nil), mi.Value()...),
			})
		}
		mi.Next()
	}
	return results, nil
}

// EngineStats holds a snapshot of engine statistics for external inspection.
type EngineStats struct {
	MemtableSize    int   `json:"memtableSize"`
	ImmutableCount  int   `json:"immutableCount"`
	LevelFileCounts []int `json:"levelFileCounts"`
	TotalSSTables   int   `json:"totalSstables"`
	MemoryBytes     int   `json:"memoryBytes"` // total estimated memory usage
}

// Stats returns a point-in-time snapshot of the engine statistics.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	stats := EngineStats{
		MemtableSize:    e.memtable.ApproximateSize(),
		ImmutableCount:  len(e.immutableMems),
		LevelFileCounts: make([]int, len(e.levels)),
	}
	for i, lvl := range e.levels {
		stats.LevelFileCounts[i] = len(lvl)
		stats.TotalSSTables += len(lvl)
	}
	stats.MemoryBytes = e.memtable.ApproximateSize()
	for _, imm := range e.immutableMems {
		stats.MemoryBytes += imm.ApproximateSize()
	}
	return stats
}

// Close shuts down the engine gracefully.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.closeCh)
	e.wg.Wait()

	// Flush remaining memtable to L0 if it has data.
	e.mu.Lock()
	if e.memtable.ApproximateSize() > 0 {
		e.memtable.Freeze()
		task := &flushTask{mem: e.memtable}
		e.mu.Unlock()
		e.flushMemtable(task)
	} else {
		e.mu.Unlock()
	}

	// Flush remaining immutable memtables.
	e.mu.Lock()
	for len(e.immutableMems) > 0 {
		imm := e.immutableMems[len(e.immutableMems)-1]
		e.mu.Unlock()
		e.flushMemtable(&flushTask{mem: imm})
		e.mu.Lock()
	}
	e.mu.Unlock()

	var firstErr error
	if err := e.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	e.closeReaders()
	if err := e.manifest.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (e *Engine) closeReaders() {
	for _, lvl := range e.levels {
		for _, r := range lvl {
			r.Close()
		}
	}
}

// maybeScheduleFlush checks if the active memtable should be flushed.
// Must be called with e.mu held (write lock).
func (e *Engine) maybeScheduleFlush() {
	if !e.memtable.ShouldFlush() {
		return
	}
	e.memtable.Freeze()
	frozen := e.memtable

	// Rotate WAL.
	oldWALID, err := e.wal.Rotate()
	if err != nil {
		// If rotation fails, we still need to keep going with a new memtable.
		oldWALID = 0
	}

	// Prepend to immutable list (newest first).
	e.immutableMems = append([]*memtable.Memtable{frozen}, e.immutableMems...)

	// Create new active memtable.
	e.memtable = memtable.NewMemtable(e.opts.MemtableSize)

	// Send flush task.
	select {
	case e.flushCh <- &flushTask{mem: frozen, walID: oldWALID}:
	default:
		// Channel full; flush will happen eventually.
	}
}

// flushLoop runs in a goroutine, processing flush tasks.
func (e *Engine) flushLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.closeCh:
			// Drain remaining tasks.
			for {
				select {
				case task := <-e.flushCh:
					e.flushMemtable(task)
				default:
					return
				}
			}
		case task := <-e.flushCh:
			e.flushMemtable(task)
		}
	}
}

// flushMemtable writes a frozen memtable to an L0 SSTable.
func (e *Engine) flushMemtable(task *flushTask) {
	fileNum := e.allocFileNum()
	path := e.sstPath(fileNum)

	writer, err := sstable.NewWriter(path, e.opts.BlockSize)
	if err != nil {
		return
	}

	it := task.mem.NewIterator()
	it.SeekToFirst()
	for it.Valid() {
		if err := writer.Add(it.Key(), it.Value(), it.IsTombstone()); err != nil {
			return
		}
		it.Next()
	}

	if writer.EntryCount() == 0 {
		os.Remove(path)
		e.removeImmutable(task.mem)
		return
	}

	if err := writer.Finish(); err != nil {
		os.Remove(path)
		return
	}

	// Open reader.
	reader, err := sstable.OpenReader(path)
	if err != nil {
		os.Remove(path)
		return
	}

	// Get file size.
	fi, err := os.Stat(path)
	var fileSize int64
	if err == nil {
		fileSize = fi.Size()
	}

	meta := FileMetadata{
		FileNum: fileNum,
		Level:   0,
		Size:    fileSize,
		MinKey:  reader.MinKey(),
		MaxKey:  reader.MaxKey(),
	}

	if err := e.manifest.AddFile(meta); err != nil {
		reader.Close()
		os.Remove(path)
		return
	}

	e.mu.Lock()
	e.levels[0] = append(e.levels[0], reader)
	e.mu.Unlock()

	e.removeImmutable(task.mem)

	// Purge old WAL.
	if task.walID > 0 {
		e.wal.PurgeOlderThan(task.walID)
	}

	// Signal compaction check.
	select {
	case e.compactCh <- struct{}{}:
	default:
	}
}

func (e *Engine) removeImmutable(mem *memtable.Memtable) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, imm := range e.immutableMems {
		if imm == mem {
			e.immutableMems = append(e.immutableMems[:i], e.immutableMems[i+1:]...)
			return
		}
	}
}

// findSSTableForKey finds the SSTable in the given level whose key range
// contains the target key, using binary search on manifest metadata.
func (e *Engine) findSSTableForKey(level int, key []byte) *sstable.Reader {
	readers := e.levels[level]
	if len(readers) == 0 {
		return nil
	}

	// For L1+, files are sorted by key range and non-overlapping.
	// Binary search for the file whose min <= key <= max.
	metas := e.manifest.GetLevel(level)
	if len(metas) == 0 {
		return nil
	}

	// Sort metas by MinKey to ensure correct binary search.
	sort.Slice(metas, func(i, j int) bool {
		return bytes.Compare(metas[i].MinKey, metas[j].MinKey) < 0
	})

	// Find the last SSTable whose MinKey <= key.
	idx := sort.Search(len(metas), func(i int) bool {
		return bytes.Compare(metas[i].MinKey, key) > 0
	}) - 1

	if idx < 0 {
		return nil
	}

	// Check if key <= MaxKey of this SSTable.
	if bytes.Compare(key, metas[idx].MaxKey) > 0 {
		return nil
	}

	// Find the corresponding reader by matching key range.
	for _, r := range readers {
		if r.MinKey() != nil && r.MaxKey() != nil {
			rMeta := metas[idx]
			if bytes.Equal(r.MinKey(), rMeta.MinKey) && bytes.Equal(r.MaxKey(), rMeta.MaxKey) {
				return r
			}
		}
	}

	// Fallback: linear search through readers.
	for _, r := range readers {
		if r.MinKey() == nil || r.MaxKey() == nil {
			continue
		}
		if bytes.Compare(key, r.MinKey()) >= 0 && bytes.Compare(key, r.MaxKey()) <= 0 {
			return r
		}
	}

	return nil
}
