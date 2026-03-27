package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// RecordType indicates the kind of WAL record.
type RecordType byte

const (
	RecordPut    RecordType = 0x01
	RecordDelete RecordType = 0x02
)

const defaultMaxSize = 64 * 1024 * 1024 // 64MB

// Record represents a single WAL entry.
type Record struct {
	Type  RecordType
	Key   []byte
	Value []byte // nil for Delete
}

// WAL is an append-only write-ahead log backed by sequentially numbered files.
type WAL struct {
	mu      sync.Mutex
	file    *os.File
	dir     string
	fileID  uint64
	size    int64
	maxSize int64
	buf     *bufio.Writer
}

// Open opens or creates a WAL in the given directory. If maxSize <= 0 the
// default of 64 MB is used.
func Open(dir string, maxSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	if maxSize <= 0 {
		maxSize = defaultMaxSize
	}

	ids, err := findWALFiles(dir)
	if err != nil {
		return nil, err
	}

	var fileID uint64 = 1
	if len(ids) > 0 {
		fileID = ids[len(ids)-1]
	}

	w := &WAL{
		dir:     dir,
		fileID:  fileID,
		maxSize: maxSize,
	}

	if err := w.openFile(); err != nil {
		return nil, err
	}

	return w, nil
}

// walFileName returns the filename for a given file ID.
func walFileName(id uint64) string {
	return fmt.Sprintf("%06d.wal", id)
}

// openFile opens (or creates) the active WAL file for appending.
func (w *WAL) openFile() error {
	path := filepath.Join(w.dir, walFileName(w.fileID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("wal: stat: %w", err)
	}

	w.file = f
	w.size = info.Size()
	w.buf = bufio.NewWriter(f)
	return nil
}

// Append encodes and writes a single record, then flushes and fsyncs.
func (w *WAL) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeRecord(rec)
	n, err := w.buf.Write(data)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.size += int64(n)

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// AppendBatch writes multiple records with a single flush + fsync.
func (w *WAL) AppendBatch(recs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, rec := range recs {
		data := encodeRecord(rec)
		n, err := w.buf.Write(data)
		if err != nil {
			return fmt.Errorf("wal: write: %w", err)
		}
		w.size += int64(n)
	}

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// Sync explicitly flushes and fsyncs the active WAL file.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	return w.file.Sync()
}

// Rotate closes the current WAL file, increments the fileID, and opens a new
// file. It returns the OLD fileID.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return 0, fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return 0, fmt.Errorf("wal: close: %w", err)
	}

	oldID := w.fileID
	w.fileID++

	if err := w.openFile(); err != nil {
		return 0, err
	}
	return oldID, nil
}

// Replay reads all WAL files in order and calls fn for each valid record.
// On CRC mismatch or short read the current file is considered to have tail
// corruption and replay moves on. Records already delivered are not revoked.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := findWALFiles(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if err := replayFile(filepath.Join(w.dir, walFileName(id)), fn); err != nil {
			return err
		}
	}
	return nil
}

// replayFile reads records from a single WAL file. It tolerates tail
// corruption by stopping on CRC mismatch or unexpected EOF.
func replayFile(path string, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: open for replay: %w", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	header := make([]byte, 8) // CRC(4) + DataLen(4)

	for {
		// Read header.
		if _, err := io.ReadFull(r, header); err != nil {
			// EOF or short read in header — done with this file.
			break
		}

		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		dataLen := binary.LittleEndian.Uint32(header[4:8])

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(r, data); err != nil {
			// Truncated record — stop.
			break
		}

		// Verify CRC.
		if crc32.ChecksumIEEE(data) != storedCRC {
			// Corruption detected — stop reading this file.
			break
		}

		rec, ok := decodeRecordData(data)
		if !ok {
			break
		}

		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// decodeRecordData parses the payload portion (Type + Key + Value) of a
// record. Returns false if the data is too short.
func decodeRecordData(data []byte) (Record, bool) {
	// Minimum: Type(1) + KeyLen(4) + ValueLen(4) = 9
	if len(data) < 9 {
		return Record{}, false
	}

	typ := RecordType(data[0])
	off := 1

	keyLen := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4

	if off+int(keyLen) > len(data) {
		return Record{}, false
	}
	key := make([]byte, keyLen)
	copy(key, data[off:off+int(keyLen)])
	off += int(keyLen)

	if off+4 > len(data) {
		return Record{}, false
	}
	valLen := binary.LittleEndian.Uint32(data[off : off+4])
	off += 4

	if off+int(valLen) > len(data) {
		return Record{}, false
	}

	var val []byte
	if valLen > 0 {
		val = make([]byte, valLen)
		copy(val, data[off:off+int(valLen)])
	}

	return Record{Type: typ, Key: key, Value: val}, true
}

// PurgeOlderThan deletes all WAL files with ID <= fileID.
func (w *WAL) PurgeOlderThan(fileID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, err := findWALFiles(w.dir)
	if err != nil {
		return err
	}

	for _, id := range ids {
		if id <= fileID {
			path := filepath.Join(w.dir, walFileName(id))
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("wal: purge %s: %w", path, err)
			}
		}
	}
	return nil
}

// Close flushes, syncs, and closes the active WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.buf != nil {
		if err := w.buf.Flush(); err != nil {
			return fmt.Errorf("wal: flush: %w", err)
		}
	}
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
		return w.file.Close()
	}
	return nil
}

// encodeRecord serialises a Record into its binary on-disk format:
//
//	[CRC32:4][DataLen:4][Type:1][KeyLen:4][Key:var][ValueLen:4][Value:var]
func encodeRecord(rec Record) []byte {
	keyLen := len(rec.Key)
	valLen := len(rec.Value)
	// data = Type(1) + KeyLen(4) + Key + ValueLen(4) + Value
	dataSize := 1 + 4 + keyLen + 4 + valLen
	total := 4 + 4 + dataSize // CRC + DataLen + data

	out := make([]byte, total)

	// Fill data portion first (starting at offset 8).
	off := 8
	out[off] = byte(rec.Type)
	off++
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(keyLen))
	off += 4
	copy(out[off:off+keyLen], rec.Key)
	off += keyLen
	binary.LittleEndian.PutUint32(out[off:off+4], uint32(valLen))
	off += 4
	copy(out[off:off+valLen], rec.Value)

	// DataLen.
	binary.LittleEndian.PutUint32(out[4:8], uint32(dataSize))

	// CRC over the data portion.
	crc := crc32.ChecksumIEEE(out[8:])
	binary.LittleEndian.PutUint32(out[0:4], crc)

	return out
}

// findWALFiles returns sorted file IDs found in dir.
func findWALFiles(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wal: readdir: %w", err)
	}

	var ids []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".wal") {
			continue
		}
		numStr := strings.TrimSuffix(name, ".wal")
		id, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
