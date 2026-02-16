package main

import (
	"io"
	"net/http"
)

type HTTPHandler struct {
	kvStore *KVStore
}

func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	switch r.Method {
	case http.MethodPut:
		value, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.kvStore.Propose(key, string(value))
	case http.MethodGet:
		value, exists := h.kvStore.Get(key)
		if !exists {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}
		w.Write([]byte(value))
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
