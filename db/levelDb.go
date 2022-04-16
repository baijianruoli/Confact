package db

import (
	"confact1/arrays"
	pb "confact1/confact/proto"
	"confact1/util"
	"encoding/json"
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"sync"
)

var Db *leveldb.DB
var once sync.Once

func ConfactDbInit() {
	once.Do(func() {
		Db, _ = leveldb.OpenFile("leveldb", nil)
	})
}

func LevelDBSaveLogs(logEntry *pb.LogEntry) {
	var (
		dbEntry   []byte
		err       error
		nodeEntry *Node
	)
	dbEntry, err = Db.Get(util.StringToByte(logEntry.Command.Key), nil)
	if err != nil {
		nodeEntry = &Node{
			Key:        logEntry.Command.Key,
			ValuesList: make([]*pb.LogEntry, 0),
			LockList:   make([]*pb.LogEntry, 0),
			WriteList:  make([]*pb.LogEntry, 0),
		}
	} else {
		if err = json.Unmarshal(dbEntry, &nodeEntry); err != nil {
			log.Printf("[LevelDBSaveLogs] json.Unmarshal error.err:%s\n", err.Error())
			return
		}
	}
	// 二分插入
	switch logEntry.Command.LogType {
	case pb.LogType_DATA:
		nodeEntry.ValuesList = arrays.DataBinaryDomain.Insert(nodeEntry.ValuesList, logEntry)
	case pb.LogType_LOCK:
		nodeEntry.LockList = append(nodeEntry.LockList, logEntry)
	case pb.LogType_WRITE:
		nodeEntry.WriteList = arrays.WriteBinaryDomain.Insert(nodeEntry.WriteList, logEntry)

	}
	data, _ := json.Marshal(nodeEntry)
	Db.Put(util.StringToByte(logEntry.Command.Key), data, nil)
}
