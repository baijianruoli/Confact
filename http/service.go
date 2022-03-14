package http

import (
	"confact1/conf"
	"confact1/db"
	rpc "confact1/rpc"
	"encoding/json"
	"fmt"
	pb "github.com/baijianruoli/conf/confact/proto"
	"io/ioutil"
	"net/http"
)

func GetHandler(w http.ResponseWriter, r *http.Request) {
	v := r.URL.Query()
	key := v.Get("key")
	rep, err := db.Db.Get([]byte(key), nil)
	if err != nil {
		fmt.Fprintf(w, "值不存在")
		return
	}
	fmt.Fprint(w, string(rep))
}

func SetHandler(w http.ResponseWriter, r *http.Request) {
	if !rpc.Rf.Leader {
		fmt.Fprintf(w, "当前节点不是Leader，不接受写操作")
		return
	}
	content, err := ioutil.ReadAll(r.Body)
	res := &conf.Res{}
	json.Unmarshal(content, res)
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}
	log := pb.LogEntry{
		Term:    rpc.Rf.CurrentTerm,
		Index:   int64(len(rpc.Rf.Log)),
		Command: &pb.Entry{Key: res.Key, Value: []byte(res.Value)},
	}
	rpc.Rf.Log = append(rpc.Rf.Log, &log)
	rpc.Rf.AppendLogs(res)
	rpc.Rf.Persist()
	fmt.Fprintf(w, "ok")

}

// 删除功能暂时不做
func DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

// 获取节点级别信息
func InfoHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, rpc.Rf.CurrentTerm)
}
