package sstable

import "bytes"

// entry represents a single key-value pair in a data block.
type entry struct {
	key       []byte
	value     []byte
	tombstone bool
}

// Iterator iterates over entries in an SSTable in sorted key order.
type Iterator struct {
	reader   *Reader
	blockIdx int
	entries  []entry // decoded entries of current block
	entryIdx int
	valid    bool
}

// SeekToFirst positions the iterator at the first entry in the SSTable.
func (it *Iterator) SeekToFirst() {
	if len(it.reader.index) == 0 {
		it.valid = false
		return
	}
	if err := it.loadBlock(0); err != nil {
		it.valid = false
		return
	}
	it.entryIdx = 0
	it.valid = len(it.entries) > 0
}

// Seek positions the iterator at the first entry with key >= target.
func (it *Iterator) Seek(target []byte) {
	if len(it.reader.index) == 0 {
		it.valid = false
		return
	}

	// Binary search for the first block whose lastKey >= target.
	lo, hi := 0, len(it.reader.index)-1
	blockIdx := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(it.reader.index[mid].lastKey, target) >= 0 {
			blockIdx = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	if blockIdx == -1 {
		it.valid = false
		return
	}

	if err := it.loadBlock(blockIdx); err != nil {
		it.valid = false
		return
	}

	// Linear scan to find first entry >= target.
	for i, e := range it.entries {
		if bytes.Compare(e.key, target) >= 0 {
			it.entryIdx = i
			it.valid = true
			return
		}
	}

	// All entries in this block are < target; try next block.
	it.blockIdx = blockIdx
	it.entryIdx = len(it.entries) // force Next to load next block
	it.valid = true
	it.Next()
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	it.entryIdx++
	if it.entryIdx >= len(it.entries) {
		// Move to next block.
		nextBlock := it.blockIdx + 1
		if nextBlock >= len(it.reader.index) {
			it.valid = false
			return
		}
		if err := it.loadBlock(nextBlock); err != nil {
			it.valid = false
			return
		}
		it.entryIdx = 0
		if len(it.entries) == 0 {
			it.valid = false
		}
	}
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *Iterator) Valid() bool {
	return it.valid
}

// Key returns the key of the current entry.
func (it *Iterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.entries[it.entryIdx].key
}

// Value returns the value of the current entry. Nil for tombstones.
func (it *Iterator) Value() []byte {
	if !it.valid {
		return nil
	}
	return it.entries[it.entryIdx].value
}

// IsTombstone returns true if the current entry is a deletion marker.
func (it *Iterator) IsTombstone() bool {
	if !it.valid {
		return false
	}
	return it.entries[it.entryIdx].tombstone
}

// loadBlock decodes the data block at the given index.
func (it *Iterator) loadBlock(idx int) error {
	ie := it.reader.index[idx]
	entries, err := it.reader.decodeBlock(int(ie.offset), ie.blockSize)
	if err != nil {
		return err
	}
	it.blockIdx = idx
	it.entries = entries
	return nil
}
