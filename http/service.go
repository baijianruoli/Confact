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
	"sync"
	"time"
)

type SetTypeEntity struct {
	RaftID   int64       `json:"raft_id"`
	LogEntry pb.LogEntry `json:"log_entry"`
}

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
	ts := v.Get("ts")

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
	startTs, _ := strconv.Atoi(ts)
	entry := arrays.WriteBinaryDomain.LowerSearchNode(node.WriteList, int64(startTs))
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

func ScanHandler(w http.ResponseWriter, r *http.Request) {

}

func GetBatchHandler(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ResponseInfo(500, err.Error(), w)
		return
	}
	var reqs []*conf.Req
	if jsonErr := json.Unmarshal(content, &reqs); jsonErr != nil {
		ResponseInfo(500, jsonErr.Error(), w)
		return
	}
	responses := make([]interface{}, 0)
	for _, item := range reqs {
		rep, err := db.Db.Get(util.StringToByte(item.Key), nil)
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
		responses = append(responses, response)
	}
	ResponseInfo(200, responses, w)
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

func SetTypeHandler(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ResponseInfo(500, err.Error(), w)
		return
	}
	var req SetTypeEntity
	if jsonErr := json.Unmarshal(content, &req); jsonErr != nil {
		ResponseInfo(500, jsonErr.Error(), w)
		return
	}

	rf := rpc.GetRaftService(req.RaftID)

	if !rf.Leader {
		ResponseInfo(500, "当前节点不是Leader，不接受写操作", w)
		return
	}

	req.LogEntry.Term = rf.CurrentTerm
	req.LogEntry.Index = rf.GetLastIndex()
	//追加日志，无论是write，lock，value还是delete_lock
	rf.Log = append(rf.Log, &req.LogEntry)

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

func SetBatchHandler(w http.ResponseWriter, r *http.Request) {

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ResponseInfo(500, err.Error(), w)
		return
	}
	var reqs []*conf.Req
	if jsonErr := json.Unmarshal(content, &reqs); jsonErr != nil {
		ResponseInfo(500, jsonErr.Error(), w)
		return
	}
	// 对raftID分组
	raftIdMap := make(map[int64][]*conf.Req)
	for _, item := range reqs {
		if _, ok := raftIdMap[item.RaftID]; !ok {
			list := make([]*conf.Req, 0)
			list = append(list, item)
			raftIdMap[item.RaftID] = list
		} else {
			raftIdMap[item.RaftID] = append(raftIdMap[item.RaftID], item)
		}
	}

	// 每一个raftID 启动一个协程处理
	var wg sync.WaitGroup
	for k, v := range raftIdMap {
		wg.Add(1)
		go func(k int64, v []*conf.Req) {
			rf := rpc.GetRaftService(k)
			if !rf.Leader {
				ResponseInfo(500, "当前节点不是Leader，不接受写操作", w)
				return
			}
			for _, item := range v {
				startTs := time.Now().UnixNano() / 1e6
				AppendLog(pb.LogType_DATA, item, startTs)
				AppendLog(pb.LogType_WRITE, item, startTs)
			}
			rf.AppendLogs()
			go rf.Persist()
			wg.Done()
		}(k, v)
	}
	wg.Wait()
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

// 获取key的详细信息
func GetDetailHandler(w http.ResponseWriter, r *http.Request) {
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
	ResponseInfo(200, node, w)
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
