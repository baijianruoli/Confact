package conf

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"log"
	"sync"
)

var RaftConf RaftConfig

var JsonConf JsonConfig

var once sync.Once

type RaftConfig struct {
	Me int64
}

func ConfigInit() {
	once.Do(func() {
		data, err := ioutil.ReadFile("./multi-raft.json")
		if err != nil {
			log.Println(err.Error())
			return
		}
		if err := json.Unmarshal(data, &JsonConf); err != nil {
			log.Println(err.Error())
			return
		}
		toml.DecodeFile("peers.toml", &RaftConf)
	})
}

type Req struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}
