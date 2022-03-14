package conf

import (
	"github.com/BurntSushi/toml"
	"sync"
)

var Conf TomlConfig

var once sync.Once

type TomlConfig struct {
	ServerUrl string
	Peers     []string
	Me        int64
}

func ConfigInit() {
	once.Do(func() {
		toml.DecodeFile("peers.toml", &Conf)
	})
}

type Res struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
