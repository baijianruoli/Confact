package http

import (
	"confact1/arrays"
	"confact1/common"
	"confact1/conf"
	pb "confact1/confact/proto"
	"confact1/db"
	rpc "confact1/rpc"
	"confact1/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

func ResponseInfo(code int64, msg interface{}, w http.ResponseWriter) {
	response := &conf.Response{
		Code: code,
		Msg:  msg,
	}
	data, _ := json.Marshal(response)
	fmt.Fprint(w, util.ByteToString(data))
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rep, err := db.Db.Get(util.StringToByte(key), nil)
	if err != nil {
		ResponseInfo(500, "值不存在", w)
		return
	}
	node := &db.Node{}
	if jsonErr := json.Unmarshal(rep, &node); jsonErr != nil {
		ResponseInfo(500, "系统错误", w)
		return
	}
	ts := time.Now().UnixNano() / 1e6
	entry := arrays.WriteBinaryDomain.LowerSearchNode(node.WriteList, ts)
	if entry == nil {
		ResponseInfo(500, "value不存在", w)
		return
	}
	entry = arrays.DataBinaryDomain.LowerSearchNode(node.ValuesList, entry.Command.Write.StartTs)
	var response interface{}
	if err := json.Unmarshal(entry.Command.Values.Value, &response); err != nil {
		ResponseInfo(500, err.Error(), w)
	}
	ResponseInfo(200, response, w)
}

func SetHandler(w http.ResponseWriter, r *http.Request) {

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ResponseInfo(500, err.Error(), w)
		return
	}
	var req conf.Req
	if jsonErr := json.Unmarshal(content, &req); jsonErr != nil {
		ResponseInfo(500, jsonErr.Error(), w)
		return
	}

	rf := rpc.GetRaftService(req.RaftID)

	if !rf.Leader {
		ResponseInfo(500, "当前节点不是Leader，不接受写操作", w)
		return
	}

	startTs := time.Now().UnixNano() / 1e6
	AppendLog(pb.LogType_DATA, &req, startTs)
	AppendLog(pb.LogType_WRITE, &req, startTs)
	fmt.Println("SET Handle time:", startTs)

	// 批量更新
	rf.Mu.Lock()
	if rf.BatchTimer == nil {
		go func() {
			// 为什么要维护批量更新
			rf.BatchTimer = time.NewTimer(time.Duration(100 * 1e6))
			rf.Mu.Unlock()
			<-rf.BatchTimer.C
			rf.AppendLogs()
			rf.BatchTimer = nil
		}()
	} else {
		fmt.Println("批量更新等待", time.Now().Unix()/1e6)
		rf.BatchTimer.Reset(time.Duration(100 * 1e6))
		rf.Mu.Unlock()
	}
	rf.Persist()
	ResponseInfo(200, "ok", w)
}

// 删除功能暂时不做
func DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

// 获取节点级别信息
func InfoHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	raftID := v.Get("raft_id")
	raftIDs, _ := strconv.Atoi(raftID)
	rf := rpc.GetRaftService(int64(raftIDs))
	ResponseInfo(200, rf.CurrentTerm, w)
}

func AppendLog(logType pb.LogType, req *conf.Req, startTs int64) {
	rf := rpc.GetRaftService(req.RaftID)
	switch logType {
	case pb.LogType_DATA:
		binaryData, _ := json.Marshal(req.Value)
		log := pb.LogEntry{
			Term:    rf.CurrentTerm,
			Index:   rf.GetLastIndex(),
			Command: &pb.Entry{Key: req.Key, LogType: logType, Values: &pb.Values{Value: binaryData, StartTs: startTs}},
		}
		rf.Log = append(rf.Log, &log)

	case pb.LogType_WRITE:
		log := pb.LogEntry{
			Term:    rf.CurrentTerm,
			Index:   rf.GetLastIndex(),
			Command: &pb.Entry{Key: req.Key, LogType: logType, Write: &pb.Write{StartTs: startTs, CommitTs: startTs}},
		}
		rf.Log = append(rf.Log, &log)
	}
}

func GetDetailHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rep, err := db.Db.Get(util.StringToByte(key), nil)
	if err != nil {
		ResponseInfo(500, "值不存在", w)
		return
	}
	var response interface{}
	if err := json.Unmarshal(rep, &response); err != nil {
		ResponseInfo(500, err.Error(), w)
	}
	ResponseInfo(200, response, w)
}

func GetSnapshot(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	raftID := v.Get("raft_id")
	raftIDs, _ := strconv.Atoi(raftID)
	rf := rpc.GetRaftService(int64(raftIDs))
	rep, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
	if err != nil {
		ResponseInfo(500, "快照不存在", w)
		return
	}
	var response interface{}
	if err := json.Unmarshal(rep, &response); err != nil {
		ResponseInfo(500, err.Error(), w)
	}
	ResponseInfo(200, response, w)
}

func GetPersist(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	raftID := v.Get("raft_id")
	raftIDs, _ := strconv.Atoi(raftID)
	rf := rpc.GetRaftService(int64(raftIDs))
	rep, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftPersist, rf.Me)), nil)
	if err != nil {
		ResponseInfo(500, "持久化不存在", w)
		return
	}
	var response interface{}
	if err := json.Unmarshal(rep, &response); err != nil {
		ResponseInfo(500, err.Error(), w)
	}
	ResponseInfo(200, response, w)

}
