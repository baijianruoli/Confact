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
	"time"
)

func GetHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rep, err := db.Db.Get(util.StringToByte(key), nil)
	if err != nil {
		fmt.Fprintf(w, "值不存在")
		return
	}
	node := &db.Node{}
	if jsonErr := json.Unmarshal(rep, &node); jsonErr != nil {
		fmt.Fprintf(w, "系统错误")
		return
	}
	ts := time.Now().UnixNano() / 1e6
	entry := arrays.WriteBinaryDomain.LowerSearchNode(node.WriteList, ts)
	if entry == nil {
		fmt.Fprintf(w, "value不存在")
		return
	}
	entry = arrays.DataBinaryDomain.LowerSearchNode(node.ValuesList, entry.Command.Write.StartTs)
	var result interface{}
	if jsonErr := json.Unmarshal(entry.Command.Values.Value, &result); jsonErr != nil {
		fmt.Fprintf(w, "序列化错误")
		return
	}
	fmt.Fprint(w, result)
}

func SetHandler(w http.ResponseWriter, r *http.Request) {

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}
	var req conf.Req
	if jsonErr := json.Unmarshal(content, &req); jsonErr != nil {
		fmt.Fprintf(w, jsonErr.Error())
		return
	}

	rf := rpc.GetRaftService(req.Key)

	if !rf.Leader {
		fmt.Fprintf(w, "当前节点不是Leader，不接受写操作")
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
	fmt.Fprintf(w, "ok")

}

// 删除功能暂时不做
func DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

// 获取节点级别信息
func InfoHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rf := rpc.GetRaftService(key)
	fmt.Fprint(w, rf.CurrentTerm)
}

func AppendLog(logType pb.LogType, req *conf.Req, startTs int64) {
	rf := rpc.GetRaftService(req.Key)
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
		fmt.Fprintf(w, "值不存在")
		return
	}
	node := &db.Node{}
	if jsonErr := json.Unmarshal(rep, &node); jsonErr != nil {
		fmt.Fprintf(w, "系统错误")
		return
	}
	fmt.Fprint(w, node)
}

func GetSnapshot(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rf := rpc.GetRaftService(key)
	rep, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
	if err != nil {
		fmt.Fprintf(w, "快照不存在")
		return
	}
	snapshot := &rpc.Snapshot{}
	if jsonErr := json.Unmarshal(rep, &snapshot); jsonErr != nil {
		fmt.Fprintf(w, "系统错误")
		return
	}
	data, _ := json.Marshal(snapshot)
	fmt.Fprint(w, util.ByteToString(data))
}

func GetPersist(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rf := rpc.GetRaftService(key)
	rep, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftPersist, rf.Me)), nil)
	if err != nil {
		fmt.Fprintf(w, "持久化不存在")
		return
	}
	raft := &rpc.RaftService{}
	if jsonErr := json.Unmarshal(rep, &raft); jsonErr != nil {
		fmt.Fprintf(w, "系统错误")
		return
	}
	data, _ := json.Marshal(raft)
	fmt.Fprint(w, util.ByteToString(data))

}
