package http

import (
	tc "confact1/conf"
	"log"
	"net/http"
)

func Start() {
	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/set", SetHandler)
	http.HandleFunc("/delete", DeleteHandler)
	http.HandleFunc("/info", InfoHandler)

	if err := http.ListenAndServe(tc.Conf.ServerUrl, nil); err != nil {
		log.Fatal(err)
	}
}
