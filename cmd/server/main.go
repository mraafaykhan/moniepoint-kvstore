package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/raafay/kvstore/engine"
	"github.com/raafay/kvstore/server"
)

func main() {
	dir := flag.String("dir", "./data", "data directory")
	tcpAddr := flag.String("tcp", ":9000", "TCP listen address")
	httpAddr := flag.String("http", ":9001", "HTTP/Dashboard listen address")
	flag.Parse()

	eng, err := engine.Open(engine.Options{Dir: *dir})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open engine: %v\n", err)
		os.Exit(1)
	}

	tcpServer := server.NewTCPServer(*tcpAddr, eng)
	if err := tcpServer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start TCP server: %v\n", err)
		eng.Close()
		os.Exit(1)
	}
	fmt.Printf("TCP server listening on %s\n", tcpServer.Addr())

	apiServer := server.NewAPIServer(*httpAddr, eng)
	if err := apiServer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start HTTP/API server: %v\n", err)
		tcpServer.Stop()
		eng.Close()
		os.Exit(1)
	}
	fmt.Printf("Dashboard & API server listening on http://localhost%s\n", *httpAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	apiServer.Stop()
	tcpServer.Stop()
	eng.Close()
	fmt.Println("Done.")
}
