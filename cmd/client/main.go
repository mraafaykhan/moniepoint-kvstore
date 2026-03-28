package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/raafay/kvstore/client"
)

func main() {
	addr := flag.String("addr", "localhost:9000", "server address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	c, err := client.Connect(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	cmd := args[0]
	switch cmd {
	case "put":
		if len(args) != 3 {
			fmt.Fprintf(os.Stderr, "Usage: put <key> <value>\n")
			os.Exit(1)
		}
		if err := c.Put([]byte(args[1]), []byte(args[2])); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "get":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: get <key>\n")
			os.Exit(1)
		}
		val, err := c.Get([]byte(args[1]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s\n", val)

	case "delete":
		if len(args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: delete <key>\n")
			os.Exit(1)
		}
		if err := c.Delete([]byte(args[1])); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "scan":
		if len(args) != 3 {
			fmt.Fprintf(os.Stderr, "Usage: scan <start> <end>\n")
			os.Exit(1)
		}
		pairs, err := c.GetRange([]byte(args[1]), []byte(args[2]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		for _, p := range pairs {
			fmt.Printf("%s = %s\n", p.Key, p.Value)
		}

	case "batch":
		if len(args) < 3 || len(args)%2 != 1 {
			fmt.Fprintf(os.Stderr, "Usage: batch <key1> <val1> <key2> <val2> ...\n")
			os.Exit(1)
		}
		n := (len(args) - 1) / 2
		keys := make([][]byte, n)
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			keys[i] = []byte(args[1+2*i])
			values[i] = []byte(args[2+2*i])
		}
		if err := c.BatchPut(keys, values); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: kvstore-cli -addr <addr> <command> [args]\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  put <key> <value>\n")
	fmt.Fprintf(os.Stderr, "  get <key>\n")
	fmt.Fprintf(os.Stderr, "  delete <key>\n")
	fmt.Fprintf(os.Stderr, "  scan <start> <end>\n")
	fmt.Fprintf(os.Stderr, "  batch <key1> <val1> <key2> <val2> ...\n")
}
