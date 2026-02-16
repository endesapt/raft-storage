package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	commitC := make(chan string)
	_ = join

	NewRaftNode(*id, strings.Split(*cluster, ","), proposeC, commitC)
	kv := NewKVStore(proposeC, commitC)
	handler := &HTTPHandler{kvStore: kv}

	log.Printf("KVStore listening on port %d", *kvport)
	http.ListenAndServe(fmt.Sprintf(":%d", *kvport), handler)
}
