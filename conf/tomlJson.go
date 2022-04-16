package conf

type TomlConfig struct {
	Nodes     int64
	Replicate int64
	NodesHTTP []string
}

type JsonConfig struct {
	Nodes     int64               `json:"nodes"`
	Replicate int64               `json:"replicate"`
	NodesHTTP []string            `json:"nodes_http"`
	NodesInfo map[int64]*NodeInfo `json:"nodes_info"`
	RaftsRPC  map[int64]string    `json:"rafts_rpc"`
}

type NodeInfo struct {
	NodeID    int64       `json:"node_id"`
	NodeRafts []*RaftInfo `json:"node_rafts"`
}

type RaftInfo struct {
	RaftID   int64   `json:"raft_id"`
	RaftRPC  string  `json:"raft_rpc"`
	RaftList []int64 `json:"raft_list"`
}
