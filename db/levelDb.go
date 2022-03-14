package db

import (
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
)

var Db *leveldb.DB
var once sync.Once

func ConfactDbInit() {
	once.Do(func() {
		Db, _ = leveldb.OpenFile("leveldb", nil)
	})
}
