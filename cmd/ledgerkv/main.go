// Command ledgerkv is a small CLI and line-protocol REPL over a store.
//
//	ledgerkv -dir ./data set name alice 60s
//	ledgerkv -dir ./data get name
//	ledgerkv -dir ./data repl
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/AbnormalityAI/ledgerkv"
)

func run(s *ledgerkv.Store, args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("no command")
	}
	switch strings.ToLower(args[0]) {
	case "set":
		if len(args) < 3 {
			return "", fmt.Errorf("usage: set KEY VALUE [TTL]")
		}
		var ttl time.Duration
		if len(args) > 3 {
			d, err := time.ParseDuration(args[3])
			if err != nil {
				return "", err
			}
			ttl = d
		}
		return "OK", s.Set(args[1], []byte(args[2]), ttl)
	case "get":
		if len(args) != 2 {
			return "", fmt.Errorf("usage: get KEY")
		}
		if v, ok := s.Get(args[1]); ok {
			return string(v), nil
		}
		return "(nil)", nil
	case "del":
		if len(args) != 2 {
			return "", fmt.Errorf("usage: del KEY")
		}
		return "OK", s.Delete(args[1])
	case "ttl":
		if len(args) != 2 {
			return "", fmt.Errorf("usage: ttl KEY")
		}
		if d, ok := s.TTL(args[1]); ok {
			return d.Round(time.Millisecond).String(), nil
		}
		return "(nil)", nil
	case "snapshot":
		return "OK", s.Snapshot()
	case "len":
		return fmt.Sprint(s.Len()), nil
	}
	return "", fmt.Errorf("unknown command %q", args[0])
}

func main() {
	dir := flag.String("dir", "./ledger-data", "data directory")
	flag.Parse()
	s, err := ledgerkv.Open(*dir, ledgerkv.Options{SyncEveryWrite: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer s.Close()
	args := flag.Args()
	if len(args) == 1 && args[0] == "repl" {
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			out, err := run(s, strings.Fields(sc.Text()))
			if err != nil {
				fmt.Println("ERR", err)
				continue
			}
			fmt.Println(out)
		}
		return
	}
	out, err := run(s, args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Println(out)
}
