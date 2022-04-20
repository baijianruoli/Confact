package util

import (
	"confact1/conf"
	"crypto/md5"
)

func Hash(key string) int64 {
	var ros int64
	res := md5.Sum(StringToByte(key))
	for _, item := range res {
		ros += int64(item)
	}
	return ros % conf.JsonConf.Nodes
}
