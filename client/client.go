package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/raafay/kvstore/server"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("key not found")

// KVPair holds a key-value pair.
type KVPair struct {
	Key   []byte
	Value []byte
}

// Client is a TCP client for the KV store.
type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
	mu   sync.Mutex
}

// Connect establishes a connection to the KV store server.
func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: connect: %w", err)
	}
	return &Client{
		conn: conn,
		r:    bufio.NewReader(conn),
		w:    bufio.NewWriter(conn),
	}, nil
}

// Put inserts a key-value pair.
func (c *Client) Put(key, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := server.EncodePut(key, value)
	if err := server.WriteRequest(c.w, server.CmdPut, payload); err != nil {
		return fmt.Errorf("client: put write: %w", err)
	}
	status, respPayload, err := server.ReadResponse(c.r)
	if err != nil {
		return fmt.Errorf("client: put read: %w", err)
	}
	return c.checkStatus(status, respPayload)
}

// Get retrieves the value for a key.
func (c *Client) Get(key []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := server.EncodeKey(key)
	if err := server.WriteRequest(c.w, server.CmdGet, payload); err != nil {
		return nil, fmt.Errorf("client: get write: %w", err)
	}
	status, respPayload, err := server.ReadResponse(c.r)
	if err != nil {
		return nil, fmt.Errorf("client: get read: %w", err)
	}
	if status == server.StatusNotFound {
		return nil, ErrNotFound
	}
	if status != server.StatusOK {
		return nil, fmt.Errorf("client: server error: %s", string(respPayload))
	}
	value, err := server.DecodeValue(respPayload)
	if err != nil {
		return nil, fmt.Errorf("client: get decode: %w", err)
	}
	return value, nil
}

// Delete removes a key.
func (c *Client) Delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := server.EncodeKey(key)
	if err := server.WriteRequest(c.w, server.CmdDelete, payload); err != nil {
		return fmt.Errorf("client: delete write: %w", err)
	}
	status, respPayload, err := server.ReadResponse(c.r)
	if err != nil {
		return fmt.Errorf("client: delete read: %w", err)
	}
	return c.checkStatus(status, respPayload)
}

// BatchPut inserts multiple key-value pairs.
func (c *Client) BatchPut(keys, values [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := server.EncodeBatchPut(keys, values)
	if err := server.WriteRequest(c.w, server.CmdBatchPut, payload); err != nil {
		return fmt.Errorf("client: batch write: %w", err)
	}
	status, respPayload, err := server.ReadResponse(c.r)
	if err != nil {
		return fmt.Errorf("client: batch read: %w", err)
	}
	return c.checkStatus(status, respPayload)
}

// GetRange retrieves all key-value pairs in [startKey, endKey].
func (c *Client) GetRange(startKey, endKey []byte) ([]KVPair, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	payload := server.EncodeRange(startKey, endKey)
	if err := server.WriteRequest(c.w, server.CmdGetRange, payload); err != nil {
		return nil, fmt.Errorf("client: range write: %w", err)
	}
	status, respPayload, err := server.ReadResponse(c.r)
	if err != nil {
		return nil, fmt.Errorf("client: range read: %w", err)
	}
	if status == server.StatusNotFound {
		return nil, nil
	}
	if status != server.StatusOK {
		return nil, fmt.Errorf("client: server error: %s", string(respPayload))
	}
	keys, values, err := server.DecodeKVPairs(respPayload)
	if err != nil {
		return nil, fmt.Errorf("client: range decode: %w", err)
	}
	pairs := make([]KVPair, len(keys))
	for i := range keys {
		pairs[i] = KVPair{Key: keys[i], Value: values[i]}
	}
	return pairs, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) checkStatus(status server.Status, payload []byte) error {
	switch status {
	case server.StatusOK:
		return nil
	case server.StatusNotFound:
		return ErrNotFound
	case server.StatusError:
		return fmt.Errorf("client: server error: %s", string(payload))
	default:
		return fmt.Errorf("client: unknown status: %d", status)
	}
}
