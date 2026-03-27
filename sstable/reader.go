package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"syscall"
)

// Footer holds metadata parsed from the last 48 bytes of an SSTable file.
type Footer struct {
	IndexOffset int64
	IndexSize   uint32
	BloomOffset int64
	BloomSize   uint32
	MagicNumber uint64
	Version     uint32
}

// Reader provides read access to an SSTable file using mmap.
type Reader struct {
	file   *os.File
	data   []byte // mmap'd region
	size   int64
	footer Footer
	index  []indexEntry
	bloom  *BloomFilter
	minKey []byte
	maxKey []byte
	path   string
}

// OpenReader opens an SSTable file for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()
	if size < footerSize {
		f.Close()
		return nil, errors.New("sstable: file too small to contain footer")
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: mmap failed: %w", err)
	}

	r := &Reader{
		file: f,
		data: data,
		size: size,
		path: path,
	}

	if err := r.parseFooter(); err != nil {
		r.Close()
		return nil, err
	}
	if err := r.parseIndex(); err != nil {
		r.Close()
		return nil, err
	}
	if err := r.parseBloom(); err != nil {
		r.Close()
		return nil, err
	}

	// Set minKey and maxKey from index.
	if len(r.index) > 0 {
		// minKey: first key from the first data block.
		firstBlock := r.index[0]
		entries, err := r.decodeBlock(int(firstBlock.offset), firstBlock.blockSize)
		if err == nil && len(entries) > 0 {
			r.minKey = entries[0].key
		}
		r.maxKey = r.index[len(r.index)-1].lastKey
	}

	return r, nil
}

func (r *Reader) parseFooter() error {
	footerData := r.data[r.size-footerSize:]

	// Verify footer CRC.
	storedCRC := binary.LittleEndian.Uint32(footerData[44:48])
	computedCRC := crc32.ChecksumIEEE(footerData[0:44])
	if storedCRC != computedCRC {
		return errors.New("sstable: footer CRC mismatch")
	}

	r.footer.IndexOffset = int64(binary.LittleEndian.Uint64(footerData[0:8]))
	r.footer.IndexSize = binary.LittleEndian.Uint32(footerData[8:12])
	r.footer.BloomOffset = int64(binary.LittleEndian.Uint64(footerData[12:20]))
	r.footer.BloomSize = binary.LittleEndian.Uint32(footerData[20:24])
	r.footer.MagicNumber = binary.LittleEndian.Uint64(footerData[32:40])
	r.footer.Version = binary.LittleEndian.Uint32(footerData[40:44])

	if r.footer.MagicNumber != magicNumber {
		return fmt.Errorf("sstable: bad magic number: %x", r.footer.MagicNumber)
	}
	if r.footer.Version != version {
		return fmt.Errorf("sstable: unsupported version: %d", r.footer.Version)
	}
	return nil
}

func (r *Reader) parseIndex() error {
	off := r.footer.IndexOffset
	sz := r.footer.IndexSize
	if off < 0 || int64(off)+int64(sz) > r.size {
		return errors.New("sstable: index block out of range")
	}

	indexData := r.data[off : int64(off)+int64(sz)]
	if len(indexData) < 8 {
		return errors.New("sstable: index block too small")
	}

	// Trailer is last 8 bytes: [NumBlocks:4][CRC32:4]
	trailerStart := len(indexData) - 8
	numBlocks := binary.LittleEndian.Uint32(indexData[trailerStart : trailerStart+4])
	storedCRC := binary.LittleEndian.Uint32(indexData[trailerStart+4 : trailerStart+8])
	entryData := indexData[:trailerStart]
	computedCRC := crc32.ChecksumIEEE(entryData)
	if storedCRC != computedCRC {
		return errors.New("sstable: index block CRC mismatch")
	}

	// Parse index entries.
	pos := 0
	r.index = make([]indexEntry, 0, numBlocks)
	for i := uint32(0); i < numBlocks; i++ {
		if pos+4 > len(entryData) {
			return errors.New("sstable: index entry truncated")
		}
		keyLen := int(binary.LittleEndian.Uint32(entryData[pos : pos+4]))
		pos += 4
		if pos+keyLen > len(entryData) {
			return errors.New("sstable: index key truncated")
		}
		lastKey := make([]byte, keyLen)
		copy(lastKey, entryData[pos:pos+keyLen])
		pos += keyLen
		if pos+12 > len(entryData) {
			return errors.New("sstable: index offset/size truncated")
		}
		offset := int64(binary.LittleEndian.Uint64(entryData[pos : pos+8]))
		pos += 8
		blockSize := int(binary.LittleEndian.Uint32(entryData[pos : pos+4]))
		pos += 4
		r.index = append(r.index, indexEntry{
			lastKey:   lastKey,
			offset:    offset,
			blockSize: blockSize,
		})
	}
	return nil
}

func (r *Reader) parseBloom() error {
	off := r.footer.BloomOffset
	sz := r.footer.BloomSize
	if off < 0 || int64(off)+int64(sz) > r.size {
		return errors.New("sstable: bloom block out of range")
	}

	bloomBlock := r.data[off : int64(off)+int64(sz)]
	if len(bloomBlock) < 4 {
		return errors.New("sstable: bloom block too small")
	}

	bloomData := bloomBlock[:len(bloomBlock)-4]
	storedCRC := binary.LittleEndian.Uint32(bloomBlock[len(bloomBlock)-4:])
	computedCRC := crc32.ChecksumIEEE(bloomData)
	if storedCRC != computedCRC {
		return errors.New("sstable: bloom filter CRC mismatch")
	}

	bf, err := DecodeBloomFilter(bloomData)
	if err != nil {
		return fmt.Errorf("sstable: %w", err)
	}
	r.bloom = bf
	return nil
}

// decodeBlock decodes a data block from the mmap'd region.
func (r *Reader) decodeBlock(offset int, blockSize int) ([]entry, error) {
	if offset+blockSize > len(r.data) {
		return nil, errors.New("sstable: data block out of range")
	}
	blockData := r.data[offset : offset+blockSize]
	if len(blockData) < 8 {
		return nil, errors.New("sstable: data block too small")
	}

	// Trailer: last 8 bytes [NumEntries:4][CRC32:4]
	trailerStart := len(blockData) - 8
	numEntries := int(binary.LittleEndian.Uint32(blockData[trailerStart : trailerStart+4]))
	storedCRC := binary.LittleEndian.Uint32(blockData[trailerStart+4 : trailerStart+8])
	entryData := blockData[:trailerStart]
	computedCRC := crc32.ChecksumIEEE(entryData)
	if storedCRC != computedCRC {
		return nil, errors.New("sstable: data block CRC mismatch")
	}

	entries := make([]entry, 0, numEntries)
	pos := 0
	for i := 0; i < numEntries; i++ {
		if pos+9 > len(entryData) {
			return nil, errors.New("sstable: entry header truncated")
		}
		keyLen := int(binary.LittleEndian.Uint32(entryData[pos : pos+4]))
		valLen := int(binary.LittleEndian.Uint32(entryData[pos+4 : pos+8]))
		typ := entryData[pos+8]
		pos += 9

		if pos+keyLen > len(entryData) {
			return nil, errors.New("sstable: entry key truncated")
		}
		key := make([]byte, keyLen)
		copy(key, entryData[pos:pos+keyLen])
		pos += keyLen

		var value []byte
		if typ == entryTypeValue && valLen > 0 {
			if pos+valLen > len(entryData) {
				return nil, errors.New("sstable: entry value truncated")
			}
			value = make([]byte, valLen)
			copy(value, entryData[pos:pos+valLen])
			pos += valLen
		}

		entries = append(entries, entry{
			key:       key,
			value:     value,
			tombstone: typ == entryTypeTombstone,
		})
	}

	return entries, nil
}

// Get looks up a key in the SSTable.
// Returns the value (nil for tombstones), whether the key was found, and any error.
func (r *Reader) Get(key []byte) (value []byte, found bool, err error) {
	if len(r.index) == 0 {
		return nil, false, nil
	}

	// Bloom filter check.
	if !r.bloom.MayContain(key) {
		return nil, false, nil
	}

	// Binary search in index for the first block whose lastKey >= key.
	lo, hi := 0, len(r.index)-1
	blockIdx := -1
	for lo <= hi {
		mid := lo + (hi-lo)/2
		cmp := bytes.Compare(r.index[mid].lastKey, key)
		if cmp >= 0 {
			blockIdx = mid
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}
	if blockIdx == -1 {
		return nil, false, nil
	}

	// Decode the block and scan for the key.
	ie := r.index[blockIdx]
	entries, err := r.decodeBlock(int(ie.offset), ie.blockSize)
	if err != nil {
		return nil, false, err
	}

	for _, e := range entries {
		if bytes.Equal(e.key, key) {
			if e.tombstone {
				return nil, true, nil
			}
			return e.value, true, nil
		}
	}

	return nil, false, nil
}

// MayContain checks the bloom filter for the key.
func (r *Reader) MayContain(key []byte) bool {
	return r.bloom.MayContain(key)
}

// MinKey returns the smallest key in the SSTable.
func (r *Reader) MinKey() []byte {
	return r.minKey
}

// MaxKey returns the largest key in the SSTable.
func (r *Reader) MaxKey() []byte {
	return r.maxKey
}

// NewIterator creates a new iterator over this SSTable.
func (r *Reader) NewIterator() *Iterator {
	return &Iterator{
		reader:   r,
		blockIdx: -1,
		entryIdx: -1,
		valid:    false,
	}
}

// Close unmaps the file and closes it.
func (r *Reader) Close() error {
	var firstErr error
	if r.data != nil {
		if err := syscall.Munmap(r.data); err != nil {
			firstErr = err
		}
		r.data = nil
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.file = nil
	}
	return firstErr
}
