package server_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/raafay/kvstore/client"
	"github.com/raafay/kvstore/engine"
	"github.com/raafay/kvstore/server"
)

func setupServer(t *testing.T) (*server.TCPServer, *engine.Engine) {
	t.Helper()
	dir := t.TempDir()
	eng, err := engine.Open(engine.Options{Dir: dir})
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	srv := server.NewTCPServer(":0", eng)
	if err := srv.Start(); err != nil {
		eng.Close()
		t.Fatalf("failed to start server: %v", err)
	}
	t.Cleanup(func() {
		srv.Stop()
		eng.Close()
	})
	return srv, eng
}

func connectClient(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Connect(addr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestPutAndGet(t *testing.T) {
	srv, _ := setupServer(t)
	c := connectClient(t, srv.Addr())

	if err := c.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	val, err := c.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got '%s'", val)
	}
}

func TestDelete(t *testing.T) {
	srv, _ := setupServer(t)
	c := connectClient(t, srv.Addr())

	if err := c.Put([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := c.Delete([]byte("key1")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	_, err := c.Get([]byte("key1"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestBatchPut(t *testing.T) {
	srv, _ := setupServer(t)
	c := connectClient(t, srv.Addr())

	keys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(fmt.Sprintf("bkey%02d", i))
		values[i] = []byte(fmt.Sprintf("bval%02d", i))
	}
	if err := c.BatchPut(keys, values); err != nil {
		t.Fatalf("batch put failed: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := c.Get(keys[i])
		if err != nil {
			t.Fatalf("get key %d failed: %v", i, err)
		}
		if string(val) != string(values[i]) {
			t.Fatalf("key %d: expected '%s', got '%s'", i, values[i], val)
		}
	}
}

func TestGetRange(t *testing.T) {
	srv, _ := setupServer(t)
	c := connectClient(t, srv.Addr())

	// Put keys "a" through "z"
	for ch := byte('a'); ch <= byte('z'); ch++ {
		key := []byte{ch}
		val := []byte(fmt.Sprintf("val_%c", ch))
		if err := c.Put(key, val); err != nil {
			t.Fatalf("put '%c' failed: %v", ch, err)
		}
	}

	pairs, err := c.GetRange([]byte("d"), []byte("h"))
	if err != nil {
		t.Fatalf("get range failed: %v", err)
	}
	expected := []byte{'d', 'e', 'f', 'g', 'h'}
	if len(pairs) != len(expected) {
		t.Fatalf("expected %d pairs, got %d", len(expected), len(pairs))
	}
	for i, p := range pairs {
		if len(p.Key) != 1 || p.Key[0] != expected[i] {
			t.Fatalf("pair %d: expected key '%c', got '%s'", i, expected[i], p.Key)
		}
		expectedVal := fmt.Sprintf("val_%c", expected[i])
		if string(p.Value) != expectedVal {
			t.Fatalf("pair %d: expected value '%s', got '%s'", i, expectedVal, p.Value)
		}
	}
}

func TestNotFound(t *testing.T) {
	srv, _ := setupServer(t)
	c := connectClient(t, srv.Addr())

	_, err := c.Get([]byte("nonexistent"))
	if !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestMultipleClients(t *testing.T) {
	srv, _ := setupServer(t)

	var wg sync.WaitGroup
	errs := make(chan error, 50)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			c, err := client.Connect(srv.Addr())
			if err != nil {
				errs <- fmt.Errorf("client %d connect: %v", clientID, err)
				return
			}
			defer c.Close()

			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("c%d_k%d", clientID, j))
				val := []byte(fmt.Sprintf("c%d_v%d", clientID, j))
				if err := c.Put(key, val); err != nil {
					errs <- fmt.Errorf("client %d put %d: %v", clientID, j, err)
					return
				}
			}
			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("c%d_k%d", clientID, j))
				expectedVal := fmt.Sprintf("c%d_v%d", clientID, j)
				got, err := c.Get(key)
				if err != nil {
					errs <- fmt.Errorf("client %d get %d: %v", clientID, j, err)
					return
				}
				if string(got) != expectedVal {
					errs <- fmt.Errorf("client %d key %d: expected '%s', got '%s'", clientID, j, expectedVal, got)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
