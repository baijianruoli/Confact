package main

import (
	"confact1/conf"
	pb "confact1/confact/proto"
	"confact1/db"
	"confact1/http"
	"confact1/rpc"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {

	//启动1.配置文件2.raft 3. http
	conf.ConfigInit()
	db.ConfactDbInit()

	// multi-raft
	for _, item := range conf.JsonConf.NodesInfo[conf.RaftConf.Me].NodeRafts {
		rpc.Start(item)
	}

	// 启动grpc服务
	for _, item := range conf.JsonConf.NodesInfo[conf.RaftConf.Me].NodeRafts {
		go func(rf *conf.RaftInfo) {
			s := grpc.NewServer()
			raft, _ := rpc.RaftMap.Load(rf.RaftID)
			pb.RegisterRaftServer(s, raft.(*rpc.RaftService))
			lis, err := net.Listen("tcp", conf.JsonConf.RaftsRPC[rf.RaftID])
			if err != nil {
				log.Fatal("failed to listen: %v", err)
			}
			res := s.Serve(lis)
			if res != nil {
				log.Fatal(res)
			}

		}(item)
	}

	// 启动http
	http.Start()
}
