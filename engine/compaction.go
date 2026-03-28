package engine

import (
	"bytes"
	"fmt"
	"os"

	"github.com/raafay/kvstore/sstable"
)

const (
	l1TargetSize    = 10 * 1024 * 1024  // 10 MB
	targetFileSize  = 2 * 1024 * 1024   // 2 MB per output SSTable
)

// compactionLoop runs as a background goroutine waiting for compaction signals.
func (e *Engine) compactionLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.closeCh:
			return
		case <-e.compactCh:
			e.runCompaction()
		}
	}
}

// runCompaction checks all levels and compacts where needed.
func (e *Engine) runCompaction() {
	// Check L0 first.
	e.mu.RLock()
	l0Count := len(e.levels[0])
	e.mu.RUnlock()

	if l0Count >= e.opts.L0CompactTrigger {
		if err := e.compactL0(); err != nil {
			// Log error in production; ignore here.
			_ = err
		}
	}

	// Check L1+ levels.
	for level := 1; level < e.opts.MaxLevels-1; level++ {
		if e.needsCompaction(level) {
			if err := e.compactLevel(level); err != nil {
				_ = err
			}
		}
	}
}

// needsCompaction returns true if the total size of files at the given level
// exceeds the target size for that level.
func (e *Engine) needsCompaction(level int) bool {
	if level == 0 {
		e.mu.RLock()
		count := len(e.levels[0])
		e.mu.RUnlock()
		return count >= e.opts.L0CompactTrigger
	}

	metas := e.manifest.GetLevel(level)
	var totalSize int64
	for _, m := range metas {
		totalSize += m.Size
	}
	return totalSize > e.levelTargetSize(level)
}

// levelTargetSize returns the target size for a level.
// L1=10MB, L2=100MB, L3=1GB, etc.
func (e *Engine) levelTargetSize(level int) int64 {
	size := int64(l1TargetSize)
	for i := 1; i < level; i++ {
		size *= 10
	}
	return size
}

// compactL0 merges all L0 files with overlapping L1 files.
func (e *Engine) compactL0() error {
	e.mu.RLock()
	l0Readers := make([]*sstable.Reader, len(e.levels[0]))
	copy(l0Readers, e.levels[0])
	l0Metas := e.manifest.GetLevel(0)
	e.mu.RUnlock()

	if len(l0Readers) == 0 {
		return nil
	}

	// Determine key range of all L0 files.
	var minKey, maxKey []byte
	for _, r := range l0Readers {
		if r.MinKey() == nil {
			continue
		}
		if minKey == nil || bytes.Compare(r.MinKey(), minKey) < 0 {
			minKey = r.MinKey()
		}
		if maxKey == nil || bytes.Compare(r.MaxKey(), maxKey) > 0 {
			maxKey = r.MaxKey()
		}
	}

	// Find overlapping L1 files.
	e.mu.RLock()
	l1Readers, l1Metas := e.findOverlapping(1, minKey, maxKey)
	e.mu.RUnlock()

	// Build merge iterator: L0 files get lower priority (newer), L1 files higher.
	var iters []Iterator
	var priorities []int
	// L0 newest first (reverse of slice order).
	for i := len(l0Readers) - 1; i >= 0; i-- {
		it := newSSTableIteratorAdapter(l0Readers[i].NewIterator())
		iters = append(iters, it)
		priorities = append(priorities, i) // lower index in reversed order = newer
	}
	basePriority := len(l0Readers)
	for i, r := range l1Readers {
		it := newSSTableIteratorAdapter(r.NewIterator())
		iters = append(iters, it)
		priorities = append(priorities, basePriority+i)
	}

	mi := NewMergeIterator(iters, priorities)
	mi.SeekToFirst()

	// Write new L1 SSTables.
	newMetas, newReaders, err := e.writeCompactionOutput(mi, 1)
	if err != nil {
		return err
	}

	// Under write lock: update manifest and engine state.
	e.mu.Lock()
	defer e.mu.Unlock()

	// Remove old L0 files from manifest and engine.
	for _, meta := range l0Metas {
		if err := e.manifest.RemoveFile(meta.FileNum, 0); err != nil {
			return err
		}
	}
	for _, meta := range l1Metas {
		if err := e.manifest.RemoveFile(meta.FileNum, 1); err != nil {
			return err
		}
	}

	// Add new files.
	for _, meta := range newMetas {
		if err := e.manifest.AddFile(meta); err != nil {
			return err
		}
	}

	// Update engine.levels.
	// Remove old L0 readers.
	e.levels[0] = removeReaders(e.levels[0], l0Readers)
	e.levels[1] = removeReaders(e.levels[1], l1Readers)

	// Add new readers.
	e.levels[1] = append(e.levels[1], newReaders...)

	// Close old readers and delete old files.
	for _, r := range l0Readers {
		r.Close()
	}
	for _, r := range l1Readers {
		r.Close()
	}
	for _, meta := range l0Metas {
		os.Remove(e.sstPath(meta.FileNum))
	}
	for _, meta := range l1Metas {
		os.Remove(e.sstPath(meta.FileNum))
	}

	return nil
}

// compactLevel compacts one file from level into overlapping files in level+1.
func (e *Engine) compactLevel(level int) error {
	e.mu.RLock()
	if len(e.levels[level]) == 0 {
		e.mu.RUnlock()
		return nil
	}

	// Pick the first file from this level.
	srcReader := e.levels[level][0]
	srcMeta := e.findMetaForReader(level, srcReader)
	minKey := srcReader.MinKey()
	maxKey := srcReader.MaxKey()

	targetLevel := level + 1
	targetReaders, targetMetas := e.findOverlapping(targetLevel, minKey, maxKey)
	e.mu.RUnlock()

	if srcMeta == nil {
		return nil
	}

	// Build merge iterator.
	var iters []Iterator
	var priorities []int
	iters = append(iters, newSSTableIteratorAdapter(srcReader.NewIterator()))
	priorities = append(priorities, 0) // source is newer
	for i, r := range targetReaders {
		iters = append(iters, newSSTableIteratorAdapter(r.NewIterator()))
		priorities = append(priorities, i+1)
	}

	mi := NewMergeIterator(iters, priorities)
	mi.SeekToFirst()

	newMetas, newReaders, err := e.writeCompactionOutput(mi, targetLevel)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Remove old files.
	if err := e.manifest.RemoveFile(srcMeta.FileNum, level); err != nil {
		return err
	}
	for _, meta := range targetMetas {
		if err := e.manifest.RemoveFile(meta.FileNum, targetLevel); err != nil {
			return err
		}
	}

	// Add new files.
	for _, meta := range newMetas {
		if err := e.manifest.AddFile(meta); err != nil {
			return err
		}
	}

	// Update levels.
	e.levels[level] = removeReaders(e.levels[level], []*sstable.Reader{srcReader})
	e.levels[targetLevel] = removeReaders(e.levels[targetLevel], targetReaders)
	e.levels[targetLevel] = append(e.levels[targetLevel], newReaders...)

	// Cleanup.
	srcReader.Close()
	for _, r := range targetReaders {
		r.Close()
	}
	os.Remove(e.sstPath(srcMeta.FileNum))
	for _, meta := range targetMetas {
		os.Remove(e.sstPath(meta.FileNum))
	}

	return nil
}

// findOverlapping returns readers and metadata for all SSTables in the given
// level that overlap with [minKey, maxKey]. Must be called with at least e.mu.RLock held.
func (e *Engine) findOverlapping(level int, minKey, maxKey []byte) ([]*sstable.Reader, []FileMetadata) {
	var readers []*sstable.Reader
	var metas []FileMetadata

	levelMetas := e.manifest.GetLevel(level)
	for mIdx, meta := range levelMetas {
		// Overlap if meta.MinKey <= maxKey && meta.MaxKey >= minKey.
		if bytes.Compare(meta.MinKey, maxKey) > 0 || bytes.Compare(meta.MaxKey, minKey) < 0 {
			continue
		}
		// Find corresponding reader.
		for _, r := range e.levels[level] {
			if bytes.Equal(r.MinKey(), meta.MinKey) && bytes.Equal(r.MaxKey(), meta.MaxKey) {
				readers = append(readers, r)
				metas = append(metas, levelMetas[mIdx])
				break
			}
		}
	}
	return readers, metas
}

// findMetaForReader finds the metadata for a reader in a given level.
// Must be called with at least e.mu.RLock held.
func (e *Engine) findMetaForReader(level int, reader *sstable.Reader) *FileMetadata {
	levelMetas := e.manifest.GetLevel(level)
	for i, meta := range levelMetas {
		if bytes.Equal(reader.MinKey(), meta.MinKey) && bytes.Equal(reader.MaxKey(), meta.MaxKey) {
			return &levelMetas[i]
		}
	}
	return nil
}

// writeCompactionOutput writes entries from a merge iterator into new SSTables
// for the target level, splitting at ~targetFileSize boundaries.
func (e *Engine) writeCompactionOutput(mi *MergeIterator, level int) ([]FileMetadata, []*sstable.Reader, error) {
	var metas []FileMetadata
	var readers []*sstable.Reader

	var writer *sstable.Writer
	var currentFileNum uint64
	var currentPath string
	var currentSize int64

	finishCurrent := func() error {
		if writer == nil {
			return nil
		}
		if writer.EntryCount() == 0 {
			os.Remove(currentPath)
			writer = nil
			return nil
		}
		if err := writer.Finish(); err != nil {
			os.Remove(currentPath)
			return err
		}
		reader, err := sstable.OpenReader(currentPath)
		if err != nil {
			os.Remove(currentPath)
			return err
		}
		fi, err := os.Stat(currentPath)
		var fileSize int64
		if err == nil {
			fileSize = fi.Size()
		}
		meta := FileMetadata{
			FileNum: currentFileNum,
			Level:   level,
			Size:    fileSize,
			MinKey:  reader.MinKey(),
			MaxKey:  reader.MaxKey(),
		}
		metas = append(metas, meta)
		readers = append(readers, reader)
		writer = nil
		return nil
	}

	for mi.Valid() {
		if writer == nil {
			currentFileNum = e.allocFileNum()
			currentPath = e.sstPath(currentFileNum)
			var err error
			writer, err = sstable.NewWriter(currentPath, e.opts.BlockSize)
			if err != nil {
				return nil, nil, fmt.Errorf("engine: new sstable writer: %w", err)
			}
			currentSize = 0
		}

		key := mi.Key()
		val := mi.Value()
		tomb := mi.IsTombstone()
		if err := writer.Add(key, val, tomb); err != nil {
			return nil, nil, fmt.Errorf("engine: sstable add: %w", err)
		}
		currentSize += int64(len(key) + len(val) + 9) // approximate

		mi.Next()

		if currentSize >= int64(targetFileSize) {
			if err := finishCurrent(); err != nil {
				return nil, nil, err
			}
		}
	}

	if err := finishCurrent(); err != nil {
		return nil, nil, err
	}

	return metas, readers, nil
}

// removeReaders removes specified readers from a slice.
func removeReaders(src []*sstable.Reader, toRemove []*sstable.Reader) []*sstable.Reader {
	removeSet := make(map[*sstable.Reader]bool, len(toRemove))
	for _, r := range toRemove {
		removeSet[r] = true
	}
	var result []*sstable.Reader
	for _, r := range src {
		if !removeSet[r] {
			result = append(result, r)
		}
	}
	return result
}
