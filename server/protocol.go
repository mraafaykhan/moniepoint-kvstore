package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// Command represents a wire protocol command byte.
type Command byte

const (
	CmdPut      Command = 0x01
	CmdGet      Command = 0x02
	CmdDelete   Command = 0x03
	CmdBatchPut Command = 0x04
	CmdGetRange Command = 0x05
)

// Status represents a wire protocol response status byte.
type Status byte

const (
	StatusOK       Status = 0x00
	StatusNotFound Status = 0x01
	StatusError    Status = 0x02
)

// ReadFrame reads a request frame from the reader.
// Frame format: [Length:4][Command:1][Payload:var]
// Length includes the command byte and payload.
func ReadFrame(r io.Reader) (Command, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length < 1 {
		return 0, nil, fmt.Errorf("protocol: frame too short")
	}
	frame := make([]byte, length)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, err
	}
	cmd := Command(frame[0])
	payload := frame[1:]
	return cmd, payload, nil
}

// WriteResponse writes a response frame and flushes.
// Frame format: [Length:4][Status:1][Payload:var]
func WriteResponse(w *bufio.Writer, status Status, payload []byte) error {
	length := uint32(1 + len(payload))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], length)
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if err := w.WriteByte(byte(status)); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return w.Flush()
}

// WriteRequest writes a request frame.
// Frame format: [Length:4][Command:1][Payload:var]
func WriteRequest(w *bufio.Writer, cmd Command, payload []byte) error {
	length := uint32(1 + len(payload))
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], length)
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if err := w.WriteByte(byte(cmd)); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return w.Flush()
}

// ReadResponse reads a response frame from the reader.
// Frame format: [Length:4][Status:1][Payload:var]
func ReadResponse(r io.Reader) (Status, []byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.LittleEndian.Uint32(lenBuf[:])
	if length < 1 {
		return 0, nil, fmt.Errorf("protocol: response frame too short")
	}
	frame := make([]byte, length)
	if _, err := io.ReadFull(r, frame); err != nil {
		return 0, nil, err
	}
	status := Status(frame[0])
	payload := frame[1:]
	return status, payload, nil
}

// EncodePut encodes a PUT payload: [KeyLen:4][Key][ValLen:4][Value]
func EncodePut(key, value []byte) []byte {
	buf := make([]byte, 4+len(key)+4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:4+len(key)], key)
	binary.LittleEndian.PutUint32(buf[4+len(key):8+len(key)], uint32(len(value)))
	copy(buf[8+len(key):], value)
	return buf
}

// DecodePut decodes a PUT payload.
func DecodePut(payload []byte) (key, value []byte, err error) {
	if len(payload) < 4 {
		return nil, nil, fmt.Errorf("protocol: put payload too short")
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	if len(payload) < int(4+keyLen+4) {
		return nil, nil, fmt.Errorf("protocol: put payload too short for key")
	}
	key = payload[4 : 4+keyLen]
	valLen := binary.LittleEndian.Uint32(payload[4+keyLen : 8+keyLen])
	if len(payload) < int(8+keyLen+valLen) {
		return nil, nil, fmt.Errorf("protocol: put payload too short for value")
	}
	value = payload[8+keyLen : 8+keyLen+valLen]
	return key, value, nil
}

// EncodeKey encodes a GET or DELETE payload: [KeyLen:4][Key]
func EncodeKey(key []byte) []byte {
	buf := make([]byte, 4+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(key)))
	copy(buf[4:], key)
	return buf
}

// DecodeKey decodes a GET or DELETE payload.
func DecodeKey(payload []byte) (key []byte, err error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("protocol: key payload too short")
	}
	keyLen := binary.LittleEndian.Uint32(payload[0:4])
	if len(payload) < int(4+keyLen) {
		return nil, fmt.Errorf("protocol: key payload too short for key data")
	}
	key = payload[4 : 4+keyLen]
	return key, nil
}

// EncodeBatchPut encodes a BATCH_PUT payload: [Count:4][KeyLen:4][Key][ValLen:4][Value]...
func EncodeBatchPut(keys, values [][]byte) []byte {
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

// DecodeBatchPut decodes a BATCH_PUT payload.
func DecodeBatchPut(payload []byte) (keys, values [][]byte, err error) {
	if len(payload) < 4 {
		return nil, nil, fmt.Errorf("protocol: batch payload too short")
	}
	count := binary.LittleEndian.Uint32(payload[0:4])
	off := 4
	keys = make([][]byte, 0, count)
	values = make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+4 > len(payload) {
			return nil, nil, fmt.Errorf("protocol: batch payload truncated at key len")
		}
		keyLen := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if off+int(keyLen) > len(payload) {
			return nil, nil, fmt.Errorf("protocol: batch payload truncated at key")
		}
		key := payload[off : off+int(keyLen)]
		off += int(keyLen)
		if off+4 > len(payload) {
			return nil, nil, fmt.Errorf("protocol: batch payload truncated at val len")
		}
		valLen := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if off+int(valLen) > len(payload) {
			return nil, nil, fmt.Errorf("protocol: batch payload truncated at val")
		}
		value := payload[off : off+int(valLen)]
		off += int(valLen)
		keys = append(keys, key)
		values = append(values, value)
	}
	return keys, values, nil
}

// EncodeRange encodes a GET_RANGE payload: [StartKeyLen:4][StartKey][EndKeyLen:4][EndKey]
func EncodeRange(startKey, endKey []byte) []byte {
	buf := make([]byte, 4+len(startKey)+4+len(endKey))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(startKey)))
	copy(buf[4:4+len(startKey)], startKey)
	off := 4 + len(startKey)
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(endKey)))
	copy(buf[off+4:], endKey)
	return buf
}

// DecodeRange decodes a GET_RANGE payload.
func DecodeRange(payload []byte) (startKey, endKey []byte, err error) {
	if len(payload) < 4 {
		return nil, nil, fmt.Errorf("protocol: range payload too short")
	}
	startLen := binary.LittleEndian.Uint32(payload[0:4])
	if len(payload) < int(4+startLen+4) {
		return nil, nil, fmt.Errorf("protocol: range payload too short for start key")
	}
	startKey = payload[4 : 4+startLen]
	off := 4 + startLen
	endLen := binary.LittleEndian.Uint32(payload[off : off+4])
	if len(payload) < int(off+4+endLen) {
		return nil, nil, fmt.Errorf("protocol: range payload too short for end key")
	}
	endKey = payload[off+4 : off+4+endLen]
	return startKey, endKey, nil
}

// EncodeValue encodes a GET response payload: [ValLen:4][Value]
func EncodeValue(value []byte) []byte {
	buf := make([]byte, 4+len(value))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	copy(buf[4:], value)
	return buf
}

// DecodeValue decodes a GET response payload.
func DecodeValue(payload []byte) ([]byte, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("protocol: value payload too short")
	}
	valLen := binary.LittleEndian.Uint32(payload[0:4])
	if len(payload) < int(4+valLen) {
		return nil, fmt.Errorf("protocol: value payload too short for data")
	}
	return payload[4 : 4+valLen], nil
}

// EncodeKVPairs encodes a list of key-value pairs: [Count:4][KeyLen:4][Key][ValLen:4][Value]...
func EncodeKVPairs(keys, values [][]byte) []byte {
	return EncodeBatchPut(keys, values)
}

// DecodeKVPairs decodes a list of key-value pairs.
func DecodeKVPairs(payload []byte) (keys, values [][]byte, err error) {
	return DecodeBatchPut(payload)
}
