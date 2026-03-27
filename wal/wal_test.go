package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "wal")
}

func TestWALAppendAndReplay(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Append 100 records: mix of Put and Delete.
	var expected []Record
	for i := 0; i < 100; i++ {
		rec := Record{
			Type: RecordPut,
			Key:  []byte(fmt.Sprintf("key-%04d", i)),
			Value: []byte(fmt.Sprintf("value-%04d", i)),
		}
		if i%5 == 0 {
			rec.Type = RecordDelete
			rec.Value = nil
		}
		expected = append(expected, rec)
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay.
	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open for replay: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("record count: got %d, want %d", len(got), len(expected))
	}
	for i := range expected {
		if got[i].Type != expected[i].Type {
			t.Errorf("record %d type: got %v, want %v", i, got[i].Type, expected[i].Type)
		}
		if !bytes.Equal(got[i].Key, expected[i].Key) {
			t.Errorf("record %d key mismatch", i)
		}
		if !bytes.Equal(got[i].Value, expected[i].Value) {
			t.Errorf("record %d value mismatch", i)
		}
	}
}

func TestWALBatchAppend(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var expected []Record
	for i := 0; i < 50; i++ {
		expected = append(expected, Record{
			Type:  RecordPut,
			Key:   []byte(fmt.Sprintf("bk-%04d", i)),
			Value: []byte(fmt.Sprintf("bv-%04d", i)),
		})
	}

	if err := w.AppendBatch(expected); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open for replay: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("count: got %d, want %d", len(got), len(expected))
	}
	for i := range expected {
		if !bytes.Equal(got[i].Key, expected[i].Key) || !bytes.Equal(got[i].Value, expected[i].Value) {
			t.Errorf("record %d mismatch", i)
		}
	}
}

func TestWALRotation(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var expected []Record

	// Write 10 records to first file.
	for i := 0; i < 10; i++ {
		rec := Record{Type: RecordPut, Key: []byte(fmt.Sprintf("r1-%d", i)), Value: []byte("v")}
		expected = append(expected, rec)
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Rotate.
	oldID, err := w.Rotate()
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if oldID != 1 {
		t.Fatalf("expected old fileID 1, got %d", oldID)
	}

	// Write 10 more to new file.
	for i := 0; i < 10; i++ {
		rec := Record{Type: RecordPut, Key: []byte(fmt.Sprintf("r2-%d", i)), Value: []byte("v")}
		expected = append(expected, rec)
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Replay all.
	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != len(expected) {
		t.Fatalf("count: got %d, want %d", len(got), len(expected))
	}
	for i := range expected {
		if !bytes.Equal(got[i].Key, expected[i].Key) {
			t.Errorf("record %d key: got %s, want %s", i, got[i].Key, expected[i].Key)
		}
	}
}

func TestWALCorruptionTolerance(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var expected []Record
	for i := 0; i < 20; i++ {
		rec := Record{Type: RecordPut, Key: []byte(fmt.Sprintf("ck-%d", i)), Value: []byte(fmt.Sprintf("cv-%d", i))}
		expected = append(expected, rec)
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Truncate the last few bytes of the WAL file to simulate corruption.
	path := filepath.Join(dir, walFileName(1))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	// Remove last 10 bytes — this should corrupt the last record.
	if err := os.Truncate(path, info.Size()-10); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// We should get at least 19 of the 20 records (last one is corrupted).
	if len(got) < 19 {
		t.Fatalf("expected at least 19 records, got %d", len(got))
	}
	if len(got) >= 20 {
		t.Fatalf("expected fewer than 20 records due to corruption, got %d", len(got))
	}

	// Verify the recovered records match.
	for i := range got {
		if !bytes.Equal(got[i].Key, expected[i].Key) {
			t.Errorf("record %d key mismatch", i)
		}
	}
}

func TestWALPurge(t *testing.T) {
	dir := tempDir(t)
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Create 3 files via rotation.
	w.Append(Record{Type: RecordPut, Key: []byte("a"), Value: []byte("1")})
	w.Rotate() // fileID 1 -> 2
	w.Append(Record{Type: RecordPut, Key: []byte("b"), Value: []byte("2")})
	w.Rotate() // fileID 2 -> 3
	w.Append(Record{Type: RecordPut, Key: []byte("c"), Value: []byte("3")})
	w.Close()

	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w2.Close()

	// Purge files with ID <= 2.
	if err := w2.PurgeOlderThan(2); err != nil {
		t.Fatalf("PurgeOlderThan: %v", err)
	}

	ids, err := findWALFiles(dir)
	if err != nil {
		t.Fatalf("findWALFiles: %v", err)
	}

	if len(ids) != 1 {
		t.Fatalf("expected 1 file remaining, got %d: %v", len(ids), ids)
	}
	if ids[0] != 3 {
		t.Fatalf("expected fileID 3, got %d", ids[0])
	}

	// Replay should only have the record from file 3.
	var got []Record
	if err := w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 || !bytes.Equal(got[0].Key, []byte("c")) {
		t.Fatalf("expected 1 record with key 'c', got %d records", len(got))
	}
}

func TestWALReopenExisting(t *testing.T) {
	dir := tempDir(t)

	// First session: write some records.
	w, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		w.Append(Record{Type: RecordPut, Key: []byte(fmt.Sprintf("s1-%d", i)), Value: []byte("v")})
	}
	w.Close()

	// Second session: reopen and write more.
	w2, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := 0; i < 5; i++ {
		w2.Append(Record{Type: RecordPut, Key: []byte(fmt.Sprintf("s2-%d", i)), Value: []byte("v")})
	}
	w2.Close()

	// Third session: replay all.
	w3, err := Open(dir, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer w3.Close()

	var got []Record
	if err := w3.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(got) != 10 {
		t.Fatalf("expected 10 records, got %d", len(got))
	}

	// Verify ordering: first 5 from session 1, next 5 from session 2.
	for i := 0; i < 5; i++ {
		exp := fmt.Sprintf("s1-%d", i)
		if !bytes.Equal(got[i].Key, []byte(exp)) {
			t.Errorf("record %d: got %s, want %s", i, got[i].Key, exp)
		}
	}
	for i := 0; i < 5; i++ {
		exp := fmt.Sprintf("s2-%d", i)
		if !bytes.Equal(got[5+i].Key, []byte(exp)) {
			t.Errorf("record %d: got %s, want %s", 5+i, got[5+i].Key, exp)
		}
	}
}
