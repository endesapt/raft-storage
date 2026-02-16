package main

import (
	"net/http"
)

func main() {
	proposeC := make(chan string)
	commitC := make(chan string)
	NewRaftNode(1, proposeC, commitC)
	kv := NewKVStore(proposeC, commitC)
	handler := &HTTPHandler{kvStore: kv}
	http.ListenAndServe(":8080", handler)
}
