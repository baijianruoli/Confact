package http

import (
	"confact1/conf"
	"log"
	"net/http"
)

func Start() {
	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/set", SetHandler)
	http.HandleFunc("/delete", DeleteHandler)
	http.HandleFunc("/info", InfoHandler)
	http.HandleFunc("/get/detail", GetDetailHandler)
	http.HandleFunc("/get/snapshot", GetSnapshot)
	http.HandleFunc("/get/persist", GetPersist)

	if err := http.ListenAndServe(conf.JsonConf.NodesHTTP[conf.RaftConf.Me], nil); err != nil {
		log.Fatal(err)
	}
}
