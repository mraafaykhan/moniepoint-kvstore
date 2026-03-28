package engine

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	manifestFileName = "MANIFEST"
	recordAddFile    = byte(0x01)
	recordRemoveFile = byte(0x02)
)

// FileMetadata describes an SSTable file tracked by the manifest.
type FileMetadata struct {
	FileNum uint64
	Level   int
	Size    int64
	MinKey  []byte
	MaxKey  []byte
}

// Manifest tracks which SSTable files exist at which levels.
// It is append-only on disk.
type Manifest struct {
	mu     sync.Mutex
	file   *os.File
	dir    string
	levels [][]FileMetadata
}

// OpenManifest opens or creates the MANIFEST file in dir, replays it to
// rebuild in-memory level state, and returns the Manifest.
func OpenManifest(dir string, maxLevels int) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("manifest: mkdir: %w", err)
	}

	path := filepath.Join(dir, manifestFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("manifest: open: %w", err)
	}

	m := &Manifest{
		file:   f,
		dir:    dir,
		levels: make([][]FileMetadata, maxLevels),
	}

	if err := m.replay(); err != nil {
		f.Close()
		return nil, err
	}

	// Seek to end for future appends.
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("manifest: seek: %w", err)
	}

	return m, nil
}

// replay reads the MANIFEST file from the beginning and reconstructs levels.
func (m *Manifest) replay() error {
	if _, err := m.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("manifest: seek: %w", err)
	}

	for {
		rec, err := m.readRecord()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			// Treat any other error as corruption; stop replaying.
			break
		}
		m.applyRecord(rec)
	}
	return nil
}

type manifestRecord struct {
	typ     byte
	fileNum uint64
	level   int
	size    int64
	minKey  []byte
	maxKey  []byte
}

func (m *Manifest) readRecord() (manifestRecord, error) {
	var typBuf [1]byte
	if _, err := io.ReadFull(m.file, typBuf[:]); err != nil {
		return manifestRecord{}, err
	}

	typ := typBuf[0]

	// Read FileNum(8) + Level(4) + Size(8) = 20 bytes
	var fixed [20]byte
	if _, err := io.ReadFull(m.file, fixed[:]); err != nil {
		return manifestRecord{}, err
	}

	fileNum := binary.LittleEndian.Uint64(fixed[0:8])
	level := int(binary.LittleEndian.Uint32(fixed[8:12]))
	size := int64(binary.LittleEndian.Uint64(fixed[12:20]))

	// MinKeyLen(4) + MinKey
	var mkLenBuf [4]byte
	if _, err := io.ReadFull(m.file, mkLenBuf[:]); err != nil {
		return manifestRecord{}, err
	}
	mkLen := binary.LittleEndian.Uint32(mkLenBuf[:])
	minKey := make([]byte, mkLen)
	if mkLen > 0 {
		if _, err := io.ReadFull(m.file, minKey); err != nil {
			return manifestRecord{}, err
		}
	}

	// MaxKeyLen(4) + MaxKey
	var mxLenBuf [4]byte
	if _, err := io.ReadFull(m.file, mxLenBuf[:]); err != nil {
		return manifestRecord{}, err
	}
	mxLen := binary.LittleEndian.Uint32(mxLenBuf[:])
	maxKey := make([]byte, mxLen)
	if mxLen > 0 {
		if _, err := io.ReadFull(m.file, maxKey); err != nil {
			return manifestRecord{}, err
		}
	}

	// CRC32(4)
	var crcBuf [4]byte
	if _, err := io.ReadFull(m.file, crcBuf[:]); err != nil {
		return manifestRecord{}, err
	}
	storedCRC := binary.LittleEndian.Uint32(crcBuf[:])

	// Verify CRC over everything except the CRC itself.
	data := encodeManifestRecordPayload(typ, fileNum, level, size, minKey, maxKey)
	if crc32.ChecksumIEEE(data) != storedCRC {
		return manifestRecord{}, fmt.Errorf("manifest: CRC mismatch")
	}

	return manifestRecord{
		typ:     typ,
		fileNum: fileNum,
		level:   level,
		size:    size,
		minKey:  minKey,
		maxKey:  maxKey,
	}, nil
}

func encodeManifestRecordPayload(typ byte, fileNum uint64, level int, size int64, minKey, maxKey []byte) []byte {
	// Type(1) + FileNum(8) + Level(4) + Size(8) + MinKeyLen(4) + MinKey + MaxKeyLen(4) + MaxKey
	total := 1 + 8 + 4 + 8 + 4 + len(minKey) + 4 + len(maxKey)
	buf := make([]byte, total)
	off := 0
	buf[off] = typ
	off++
	binary.LittleEndian.PutUint64(buf[off:off+8], fileNum)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(level))
	off += 4
	binary.LittleEndian.PutUint64(buf[off:off+8], uint64(size))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(minKey)))
	off += 4
	copy(buf[off:off+len(minKey)], minKey)
	off += len(minKey)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(maxKey)))
	off += 4
	copy(buf[off:off+len(maxKey)], maxKey)
	return buf
}

func (m *Manifest) applyRecord(rec manifestRecord) {
	level := rec.level
	if level < 0 || level >= len(m.levels) {
		return
	}
	switch rec.typ {
	case recordAddFile:
		m.levels[level] = append(m.levels[level], FileMetadata{
			FileNum: rec.fileNum,
			Level:   rec.level,
			Size:    rec.size,
			MinKey:  rec.minKey,
			MaxKey:  rec.maxKey,
		})
	case recordRemoveFile:
		files := m.levels[level]
		for i, f := range files {
			if f.FileNum == rec.fileNum {
				m.levels[level] = append(files[:i], files[i+1:]...)
				break
			}
		}
	}
}

// AddFile appends an AddFile record and updates in-memory state.
func (m *Manifest) AddFile(meta FileMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.writeRecord(recordAddFile, meta); err != nil {
		return err
	}
	if meta.Level >= 0 && meta.Level < len(m.levels) {
		m.levels[meta.Level] = append(m.levels[meta.Level], meta)
	}
	return nil
}

// RemoveFile appends a RemoveFile record and updates in-memory state.
func (m *Manifest) RemoveFile(fileNum uint64, level int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	meta := FileMetadata{FileNum: fileNum, Level: level}
	if err := m.writeRecord(recordRemoveFile, meta); err != nil {
		return err
	}
	if level >= 0 && level < len(m.levels) {
		files := m.levels[level]
		for i, f := range files {
			if f.FileNum == fileNum {
				m.levels[level] = append(files[:i], files[i+1:]...)
				break
			}
		}
	}
	return nil
}

func (m *Manifest) writeRecord(typ byte, meta FileMetadata) error {
	payload := encodeManifestRecordPayload(typ, meta.FileNum, meta.Level, meta.Size, meta.MinKey, meta.MaxKey)
	crc := crc32.ChecksumIEEE(payload)

	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)

	if _, err := m.file.Write(payload); err != nil {
		return fmt.Errorf("manifest: write: %w", err)
	}
	if _, err := m.file.Write(crcBuf[:]); err != nil {
		return fmt.Errorf("manifest: write crc: %w", err)
	}
	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("manifest: sync: %w", err)
	}
	return nil
}

// GetLevel returns a copy of the metadata for the given level.
func (m *Manifest) GetLevel(level int) []FileMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	if level < 0 || level >= len(m.levels) {
		return nil
	}
	result := make([]FileMetadata, len(m.levels[level]))
	copy(result, m.levels[level])
	return result
}

// GetAllLevels returns a copy of all levels' metadata.
func (m *Manifest) GetAllLevels() [][]FileMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([][]FileMetadata, len(m.levels))
	for i, lvl := range m.levels {
		result[i] = make([]FileMetadata, len(lvl))
		copy(result[i], lvl)
	}
	return result
}

// Close closes the MANIFEST file.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.file != nil {
		err := m.file.Close()
		m.file = nil
		return err
	}
	return nil
}
