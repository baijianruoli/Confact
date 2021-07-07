package main

import (
	"confact1/conf"
	"confact1/db"
	"confact1/http"
	"confact1/rpc"
	pb "github.com/baijianruoli/conf/confact/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {

	// 启动1.配置文件2.raft 3. http
	conf.ConfigInit()
	rpc.Start()
	db.ConfactDbInit()
	go http.Start()
	lis, err := net.Listen("tcp", rpc.Rf.Peers[rpc.Rf.Me])
	if err != nil {
		log.Fatal("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterRaftServer(s, rpc.Rf)
	res := s.Serve(lis)
	if res != nil {
		log.Fatal(res)
	}
}
