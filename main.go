package main

import (
	"net/http"
)

func main() {
	proposeC := make(chan string)
	commitC := make(chan string)
	go func() {
		for data := range proposeC {
			commitC <- data // Мгновенно "согласовываем"
		}
	}()
	kv := NewKVStore(proposeC, commitC)
	handler := &HTTPHandler{kvStore: kv}
	http.ListenAndServe(":8080", handler)
}
