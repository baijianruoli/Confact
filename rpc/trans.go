package rpc

import (
	"confact1/conf"
	pb "confact1/confact/proto"
	"confact1/util"
	"context"
	"google.golang.org/grpc"
	"log"
)

func (rf *RaftService) grpcClient(server int64) pb.RaftClient {
	client, ok := rf.GrpcClient.Load(server)
	if ok {
		return client.(pb.RaftClient)
	} else {
		conn, err := grpc.Dial(conf.JsonConf.RaftsRPC[server], grpc.WithInsecure())
		if err != nil {
			log.Fatal(err)
		}
		c := pb.NewRaftClient(conn)
		rf.GrpcClient.Store(server, c)
		return c
	}
}

func GetRaftService(key string) *RaftService {
	raftID := util.Hash(key)

	data, _ := RaftMap.Load(raftID)
	return data.(*RaftService)
}

// 调用rpc追加日志
func (rf *RaftService) sendAppendEntries(server int64, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, error error) {
	c := rf.grpcClient(server)
	res, err := c.AppendEntries(context.Background(), args)
	return res, err
}

// 调用rpc投票
func (rf *RaftService) sendRequestVote(server int64, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, error error) {
	c := rf.grpcClient(server)
	res, err := c.RequestVote(context.Background(), args)
	if err != nil {
		log.Println(err)
	}
	return res, err
}
