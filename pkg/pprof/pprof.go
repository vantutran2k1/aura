package pprof

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func StartServer(addr string) {
	log.Printf("starting pprof server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("error: pprof server failed: %v", err)
	}
}
