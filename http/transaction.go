package http

import (
	"confact1/arrays"
	pb "confact1/confact/proto"
	"confact1/db"
	"confact1/util"
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type ScanEntity struct {
	StartTs int64      `json:"start_ts"`
	EndTs   int64      `json:"end_ts"`
	Key     string     `json:"key"`
	Type    pb.LogType `json:"type"`
}

func TransactionReScan(w http.ResponseWriter, r *http.Request) {
	// todo 这期没时间做了
}

func TransactionScan(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ResponseInfo(500, err.Error(), w)
		return
	}
	var scanEntity *ScanEntity
	if jsonErr := json.Unmarshal(content, &scanEntity); jsonErr != nil {
		ResponseInfo(500, jsonErr.Error(), w)
		return
	}

	rep, err := db.Db.Get(util.StringToByte(scanEntity.Key), nil)
	if err != nil {
		ResponseInfo(500, "值不存在", w)
		return
	}
	node := &db.Node{}
	if jsonErr := json.Unmarshal(rep, &node); jsonErr != nil {
		ResponseInfo(500, "系统错误", w)
		return
	}

	switch scanEntity.Type {
	case pb.LogType_LOCK:
		exists := arrays.LockBinaryDomain.IsExistNode(node.LockList, scanEntity.StartTs, scanEntity.EndTs)
		ResponseInfo(200, exists, w)
	case pb.LogType_WRITE:
		exists := arrays.WriteBinaryDomain.IsExistNode(node.LockList, scanEntity.StartTs, scanEntity.EndTs)
		ResponseInfo(200, exists, w)
	}

}
