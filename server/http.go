package server

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/raafay/kvstore/engine"
)

// HTTPServer serves the admin HTTP API.
type HTTPServer struct {
	engine   *engine.Engine
	server   *http.Server
	listener net.Listener
}

// NewHTTPServer creates a new HTTP admin server.
func NewHTTPServer(addr string, eng *engine.Engine) *HTTPServer {
	s := &HTTPServer{
		engine: eng,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return s
}

// Start begins listening and serving HTTP requests.
func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.listener = ln
	go s.server.Serve(ln)
	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"running"}`))
}
