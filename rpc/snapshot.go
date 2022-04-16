package rpc

import (
	"confact1/common"
	"confact1/db"
	"confact1/util"
	"encoding/json"
	"fmt"
	"log"
)

type Snapshot struct {
	LastIndex int64
	LastTerm  int64
}

// 获取日志最后的下标
func (rf *RaftService) GetLastIndex() int64 {
	sp := &Snapshot{}
	data, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
	if err != nil {
		log.Println(err)
	}
	if errs := json.Unmarshal(data, &sp); errs != nil {
		log.Println(errs)
	}
	return sp.LastIndex + int64(len(rf.Log))
}

// 获取日志最后的term
func (rf *RaftService) GetLastTerm() int64 {
	if len(rf.Log) != 0 {
		return rf.Log[len(rf.Log)-1].Term
	}
	sp := &Snapshot{}
	data, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
	if err != nil {
		log.Println(err)
	}
	if errs := json.Unmarshal(data, &sp); errs != nil {
		log.Println(errs)
	}
	return sp.LastTerm
}

func (rf *RaftService) SnapshotInit() {
	_, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
	if err != nil {
		sp := &Snapshot{LastIndex: 0, LastTerm: 0}
		data, _ := json.Marshal(sp)
		if putErr := db.Db.Put(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), data, nil); putErr != nil {
			log.Println("[ReadPersist] put err", putErr.Error())
		}
		fmt.Println("快照初始化")
	}

}
