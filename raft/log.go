package raft

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// CommandType identifies the type of command stored in a log entry.
type CommandType byte

const (
	CmdRaftPut      CommandType = 0x01
	CmdRaftDelete   CommandType = 0x02
	CmdRaftBatchPut CommandType = 0x03
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    uint64
	Index   uint64
	CmdType CommandType
	Data    []byte
}

// --- Command encoding/decoding ---

// EncodePutCommand encodes a Put command: [KeyLen:4][Key][ValLen:4][Value]
func EncodePutCommand(key, value []byte) []byte {
	buf := make([]byte, 4+len(key)+4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:4+len(key)], key)
	binary.LittleEndian.PutUint32(buf[4+len(key):8+len(key)], uint32(len(value)))
	copy(buf[8+len(key):], value)
	return buf
}

// DecodePutCommand decodes a Put command payload.
func DecodePutCommand(data []byte) (key, value []byte) {
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	key = data[4 : 4+keyLen]
	valLen := binary.LittleEndian.Uint32(data[4+keyLen : 8+keyLen])
	value = data[8+keyLen : 8+keyLen+valLen]
	return
}

// EncodeDeleteCommand encodes a Delete command: [KeyLen:4][Key]
func EncodeDeleteCommand(key []byte) []byte {
	buf := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	return buf
}

// DecodeDeleteCommand decodes a Delete command payload.
func DecodeDeleteCommand(data []byte) (key []byte) {
	keyLen := binary.LittleEndian.Uint32(data[0:4])
	key = data[4 : 4+keyLen]
	return
}

// EncodeBatchPutCommand encodes a BatchPut command:
// [Count:4][KeyLen:4][Key][ValLen:4][Value]...
func EncodeBatchPutCommand(keys, values [][]byte) []byte {
	size := 4
	for i := range keys {
		size += 4 + len(keys[i]) + 4 + len(values[i])
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(keys)))
	off := 4
	for i := range keys {
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(keys[i])))
		off += 4
		copy(buf[off:off+len(keys[i])], keys[i])
		off += len(keys[i])
		binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(values[i])))
		off += 4
		copy(buf[off:off+len(values[i])], values[i])
		off += len(values[i])
	}
	return buf
}

// DecodeBatchPutCommand decodes a BatchPut command payload.
func DecodeBatchPutCommand(data []byte) (keys, values [][]byte) {
	count := binary.LittleEndian.Uint32(data[0:4])
	off := 4
	keys = make([][]byte, count)
	values = make([][]byte, count)
	for i := uint32(0); i < count; i++ {
		keyLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4
		keys[i] = data[off : off+int(keyLen)]
		off += int(keyLen)
		valLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4
		values[i] = data[off : off+int(valLen)]
		off += int(valLen)
	}
	return
}

// --- Persistent Log ---

const logFileName = "raft-log.bin"

// Log provides persistent storage for Raft log entries.
type Log struct {
	mu      sync.Mutex
	entries []LogEntry
	file    *os.File
	dir     string
}

// OpenLog opens or creates the raft log file in dir, replaying any existing entries.
func OpenLog(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	l := &Log{
		file: f,
		dir:  dir,
	}

	// Replay existing entries.
	if err := l.replay(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// replay reads all entries from the file into memory.
func (l *Log) replay() error {
	l.file.Seek(0, io.SeekStart)
	for {
		entry, err := l.readEntry()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			break
		}
		l.entries = append(l.entries, entry)
	}
	return nil
}

// readEntry reads a single log entry from the current file position.
// Format: [Term:8][Index:8][CmdType:1][DataLen:4][Data]
func (l *Log) readEntry() (LogEntry, error) {
	var header [21]byte
	if _, err := io.ReadFull(l.file, header[:]); err != nil {
		return LogEntry{}, err
	}
	term := binary.LittleEndian.Uint64(header[0:8])
	index := binary.LittleEndian.Uint64(header[8:16])
	cmdType := CommandType(header[16])
	dataLen := binary.LittleEndian.Uint32(header[17:21])

	data := make([]byte, dataLen)
	if dataLen > 0 {
		if _, err := io.ReadFull(l.file, data); err != nil {
			return LogEntry{}, err
		}
	}
	return LogEntry{
		Term:    term,
		Index:   index,
		CmdType: cmdType,
		Data:    data,
	}, nil
}

// writeEntry writes a single log entry to the file.
func (l *Log) writeEntry(entry LogEntry) error {
	var header [21]byte
	binary.LittleEndian.PutUint64(header[0:8], entry.Term)
	binary.LittleEndian.PutUint64(header[8:16], entry.Index)
	header[16] = byte(entry.CmdType)
	binary.LittleEndian.PutUint32(header[17:21], uint32(len(entry.Data)))

	if _, err := l.file.Write(header[:]); err != nil {
		return err
	}
	if len(entry.Data) > 0 {
		if _, err := l.file.Write(entry.Data); err != nil {
			return err
		}
	}
	return nil
}

// Append adds entries to the log and fsyncs.
func (l *Log) Append(entries ...LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, entry := range entries {
		if err := l.writeEntry(entry); err != nil {
			return fmt.Errorf("raft log: write entry: %w", err)
		}
		l.entries = append(l.entries, entry)
	}
	return l.file.Sync()
}

// GetEntry returns the entry at the given index (1-based), or nil if not found.
func (l *Log) GetEntry(index uint64) *LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index == 0 || len(l.entries) == 0 {
		return nil
	}
	// Entries should be sequential starting from some base.
	firstIdx := l.entries[0].Index
	if index < firstIdx || index > l.entries[len(l.entries)-1].Index {
		return nil
	}
	pos := index - firstIdx
	e := l.entries[pos]
	return &e
}

// LastIndex returns the index of the last log entry, or 0 if empty.
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last log entry, or 0 if empty.
func (l *Log) LastTerm() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// GetEntriesFrom returns all entries starting from the given index.
func (l *Log) GetEntriesFrom(index uint64) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 || index == 0 {
		return nil
	}
	firstIdx := l.entries[0].Index
	if index < firstIdx {
		index = firstIdx
	}
	if index > l.entries[len(l.entries)-1].Index {
		return nil
	}
	pos := index - firstIdx
	result := make([]LogEntry, len(l.entries)-int(pos))
	copy(result, l.entries[pos:])
	return result
}

// TruncateFrom removes all entries from the given index onward (inclusive).
func (l *Log) TruncateFrom(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.entries) == 0 || index == 0 {
		return nil
	}
	firstIdx := l.entries[0].Index
	if index < firstIdx {
		// Truncate everything.
		l.entries = l.entries[:0]
	} else if index <= l.entries[len(l.entries)-1].Index {
		pos := index - firstIdx
		l.entries = l.entries[:pos]
	} else {
		return nil // nothing to truncate
	}

	// Rewrite the file from scratch.
	return l.rewriteFile()
}

// rewriteFile rewrites the entire log file from the in-memory entries.
func (l *Log) rewriteFile() error {
	path := filepath.Join(l.dir, logFileName)
	l.file.Close()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	l.file = f

	for _, entry := range l.entries {
		if err := l.writeEntry(entry); err != nil {
			return err
		}
	}
	return l.file.Sync()
}

// Close closes the log file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}
