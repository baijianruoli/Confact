package db

import pb "confact1/confact/proto"

type Node struct {
	Key        string         `json:"key"`
	ValuesList []*pb.LogEntry `json:"values_list"`
	LockList   []*pb.LogEntry `json:"lock_list"`
	WriteList  []*pb.LogEntry `json:"write_list"`
	Extra      interface{}    `json:"extra"`
}
