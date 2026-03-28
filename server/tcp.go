package server

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/raafay/kvstore/engine"
)

// TCPServer serves the binary KV protocol over TCP.
type TCPServer struct {
	engine   *engine.Engine
	listener net.Listener
	addr     string
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewTCPServer creates a new TCP server bound to the given address.
func NewTCPServer(addr string, eng *engine.Engine) *TCPServer {
	return &TCPServer{
		engine:  eng,
		addr:    addr,
		closeCh: make(chan struct{}),
	}
}

// Start begins listening and accepting connections.
func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Addr returns the actual listen address (useful when using ":0").
func (s *TCPServer) Addr() string {
	if s.listener == nil {
		return s.addr
	}
	return s.listener.Addr().String()
}

// Stop closes the listener and waits for all connections to finish.
func (s *TCPServer) Stop() error {
	close(s.closeCh)
	err := s.listener.Close()
	s.wg.Wait()
	return err
}

func (s *TCPServer) acceptLoop() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.closeCh:
				return
			default:
				continue
			}
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		select {
		case <-s.closeCh:
			return
		default:
		}

		cmd, payload, err := ReadFrame(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
				return
			}
			// Check if the connection was closed.
			if opErr, ok := err.(*net.OpError); ok && opErr.Err.Error() == "use of closed network connection" {
				return
			}
			return
		}

		switch cmd {
		case CmdPut:
			s.handlePut(w, payload)
		case CmdGet:
			s.handleGet(w, payload)
		case CmdDelete:
			s.handleDelete(w, payload)
		case CmdBatchPut:
			s.handleBatchPut(w, payload)
		case CmdGetRange:
			s.handleGetRange(w, payload)
		default:
			WriteResponse(w, StatusError, []byte("unknown command"))
		}
	}
}

func (s *TCPServer) handlePut(w *bufio.Writer, payload []byte) {
	key, value, err := DecodePut(payload)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	if err := s.engine.Put(key, value); err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	WriteResponse(w, StatusOK, nil)
}

func (s *TCPServer) handleGet(w *bufio.Writer, payload []byte) {
	key, err := DecodeKey(payload)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	value, err := s.engine.Get(key)
	if err != nil {
		if errors.Is(err, engine.ErrNotFound) {
			WriteResponse(w, StatusNotFound, nil)
			return
		}
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	WriteResponse(w, StatusOK, EncodeValue(value))
}

func (s *TCPServer) handleDelete(w *bufio.Writer, payload []byte) {
	key, err := DecodeKey(payload)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	if err := s.engine.Delete(key); err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	WriteResponse(w, StatusOK, nil)
}

func (s *TCPServer) handleBatchPut(w *bufio.Writer, payload []byte) {
	keys, values, err := DecodeBatchPut(payload)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	if err := s.engine.BatchPut(keys, values); err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	WriteResponse(w, StatusOK, nil)
}

func (s *TCPServer) handleGetRange(w *bufio.Writer, payload []byte) {
	startKey, endKey, err := DecodeRange(payload)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	pairs, err := s.engine.ReadRange(startKey, endKey)
	if err != nil {
		WriteResponse(w, StatusError, []byte(err.Error()))
		return
	}
	keys := make([][]byte, len(pairs))
	values := make([][]byte, len(pairs))
	for i, p := range pairs {
		keys[i] = p.Key
		values[i] = p.Value
	}
	WriteResponse(w, StatusOK, EncodeKVPairs(keys, values))
}
