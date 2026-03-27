package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
)

const (
	defaultBlockSize = 4096
	magicNumber      = uint64(0x4B56535354424C00)
	version          = uint32(1)
	footerSize       = 48

	entryTypeValue     = byte(0x01)
	entryTypeTombstone = byte(0x02)
)

// indexEntry records the location of a data block in the SSTable file.
type indexEntry struct {
	lastKey   []byte
	offset    int64
	blockSize int
}

// Writer writes an SSTable file.
type Writer struct {
	file         *os.File
	buf          *bufio.Writer
	blockBuf     bytes.Buffer // current data block being built
	blockEntries int
	indexEntries []indexEntry
	keys         [][]byte // all keys for bloom filter
	offset       int64
	entryCount   int
	minKey       []byte
	maxKey       []byte
	blockSize    int // target block size, default 4096
}

// NewWriter creates a new SSTable writer that writes to path.
// If blockSize <= 0, it defaults to 4096.
func NewWriter(path string, blockSize int) (*Writer, error) {
	if blockSize <= 0 {
		blockSize = defaultBlockSize
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &Writer{
		file:      f,
		buf:       bufio.NewWriter(f),
		blockSize: blockSize,
	}, nil
}

// Add appends a key-value entry. Keys MUST be added in sorted order.
func (w *Writer) Add(key, value []byte, tombstone bool) error {
	// Track min/max keys.
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	if w.entryCount == 0 {
		w.minKey = keyCopy
	}
	w.maxKey = keyCopy

	w.keys = append(w.keys, keyCopy)
	w.entryCount++

	// Encode entry into blockBuf: [KeyLen:4][ValueLen:4][Type:1][Key][Value]
	var header [9]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(key)))
	if tombstone {
		binary.LittleEndian.PutUint32(header[4:8], 0)
		header[8] = entryTypeTombstone
	} else {
		binary.LittleEndian.PutUint32(header[4:8], uint32(len(value)))
		header[8] = entryTypeValue
	}
	w.blockBuf.Write(header[:])
	w.blockBuf.Write(key)
	if !tombstone {
		w.blockBuf.Write(value)
	}
	w.blockEntries++

	// Flush block if it exceeds target size.
	if w.blockBuf.Len() >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}

// flushBlock writes the current data block to the file and records an index entry.
func (w *Writer) flushBlock() error {
	if w.blockEntries == 0 {
		return nil
	}

	entryData := w.blockBuf.Bytes()
	dataCRC := crc32.ChecksumIEEE(entryData)

	// Write entry data.
	n1, err := w.buf.Write(entryData)
	if err != nil {
		return err
	}

	// Write trailer: [NumEntries:4][CRC32:4]
	var trailer [8]byte
	binary.LittleEndian.PutUint32(trailer[0:4], uint32(w.blockEntries))
	binary.LittleEndian.PutUint32(trailer[4:8], dataCRC)
	n2, err := w.buf.Write(trailer[:])
	if err != nil {
		return err
	}

	blockTotal := n1 + n2

	// Record the last key added in this block.
	lastKey := w.keys[len(w.keys)-1]

	w.indexEntries = append(w.indexEntries, indexEntry{
		lastKey:   lastKey,
		offset:    w.offset,
		blockSize: blockTotal,
	})
	w.offset += int64(blockTotal)

	w.blockBuf.Reset()
	w.blockEntries = 0
	return nil
}

// Finish flushes remaining data and writes the index block, bloom filter block,
// and footer. It fsyncs and closes the file.
func (w *Writer) Finish() error {
	// Flush any remaining block.
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Write index block.
	indexOffset := w.offset
	var indexBuf bytes.Buffer
	for _, ie := range w.indexEntries {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(ie.lastKey)))
		indexBuf.Write(hdr[:])
		indexBuf.Write(ie.lastKey)
		var off [8]byte
		binary.LittleEndian.PutUint64(off[:], uint64(ie.offset))
		indexBuf.Write(off[:])
		var bs [4]byte
		binary.LittleEndian.PutUint32(bs[:], uint32(ie.blockSize))
		indexBuf.Write(bs[:])
	}

	indexData := indexBuf.Bytes()
	indexCRC := crc32.ChecksumIEEE(indexData)

	n, err := w.buf.Write(indexData)
	if err != nil {
		return err
	}
	w.offset += int64(n)

	// Write index trailer: [NumBlocks:4][CRC32:4]
	var indexTrailer [8]byte
	binary.LittleEndian.PutUint32(indexTrailer[0:4], uint32(len(w.indexEntries)))
	binary.LittleEndian.PutUint32(indexTrailer[4:8], indexCRC)
	n2, err := w.buf.Write(indexTrailer[:])
	if err != nil {
		return err
	}
	w.offset += int64(n2)
	indexSize := uint32(w.offset - indexOffset)

	// Write bloom filter block.
	bloomOffset := w.offset
	numKeys := len(w.keys)
	if numKeys < 1 {
		numKeys = 1
	}
	bf := NewBloomFilter(numKeys, 10)
	for _, k := range w.keys {
		bf.Add(k)
	}
	bloomData := bf.Encode()
	bloomCRC := crc32.ChecksumIEEE(bloomData)

	n3, err := w.buf.Write(bloomData)
	if err != nil {
		return err
	}
	w.offset += int64(n3)

	var bloomTrailer [4]byte
	binary.LittleEndian.PutUint32(bloomTrailer[:], bloomCRC)
	n4, err := w.buf.Write(bloomTrailer[:])
	if err != nil {
		return err
	}
	w.offset += int64(n4)
	bloomSize := uint32(w.offset - bloomOffset)

	// Write 48-byte footer.
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], indexSize)
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], bloomSize)
	// Reserved: 8 bytes at [24:32], leave as zeros.
	binary.LittleEndian.PutUint64(footer[32:40], magicNumber)
	binary.LittleEndian.PutUint32(footer[40:44], version)
	footerCRC := crc32.ChecksumIEEE(footer[0:44])
	binary.LittleEndian.PutUint32(footer[44:48], footerCRC)

	if _, err := w.buf.Write(footer[:]); err != nil {
		return err
	}

	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// EntryCount returns the number of entries added so far.
func (w *Writer) EntryCount() int {
	return w.entryCount
}
