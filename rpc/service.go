package rpc

import (
	"confact1/common"
	"confact1/conf"
	"confact1/db"
	"confact1/snapshot"
	"confact1/util"
	"encoding/json"
	"fmt"
	pb "github.com/baijianruoli/conf/confact/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RaftService struct {
	Mu            sync.Mutex // Lock to protect shared access to this peer's State
	Peers         []string   // RPC end point64s of all Peers 需要持久化
	Me            int64      // this peer's index int64o Peers[]
	Dead          int64
	State         int64
	CurrentTerm   int64
	Leader        bool
	Leader_pos    int64
	Random        *rand.Rand
	ElectionTimer *time.Timer
	Log           []*pb.LogEntry

	CommitIndex int64 //已提交的最大编号
	LastApplied int64
	NextIndex   [1e6]int64
	MatchIndex  [1e6]int64
}

// 追加日志
func (rf *RaftService) AppendEntries(ctx context.Context, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, error error) {
	// 重置计数器
	rep := &pb.AppendEntriesReply{}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	clock := rf.Random.Intn(500)
	clock += 500
	rf.ElectionTimer.Reset(time.Duration(clock * 1e6))
	// 1. 类型为Leader二阶段提交
	if args.Type == 1 {
		rf.CommitIndex = int64(len(rf.Log))
		return rep, nil
	}

	// 2. 领导人版本小直接返回false
	if args.Term < rf.CurrentTerm {
		rep.Term = rf.CurrentTerm
		rep.Success = false
		return rep, nil
	}


	rf.CurrentTerm = args.Term
	// 心跳检测，直接返回
	if len(args.Entries) == 0 {
		rf.CurrentTerm = args.Term
		return rep, nil
	}
	// 3. 领导人当前没有日志
	if args.PrevLogIndex == -1 {
		rf.Log = rf.Log[:0]
		for _, v := range args.Entries {
			rf.Log = append(rf.Log, v)
		}
		rep.Success = true
		rf.Persist()
		return rep, nil
	}

	if args.Term != 0 {
		// 日志追加
		index := -1
		for k, v := range rf.Log {
			if v.Term == args.PrevLogTerm && v.Index == args.PrevLogIndex {
				index = k
				break
			}
		}
		if index == -1 {
			rep.Success = false
			return rep, nil
		}
		// 处理冲突日志后追加 index是最后匹配的日志
		rf.Log = rf.Log[0 : index+1]
		for _, v := range args.Entries {
			rf.Log = append(rf.Log, v)
		}
		rep.Success = true
		rf.Persist()
		return rep, nil
	}
	return rep, nil
}

// 投票
func (rf *RaftService) RequestVote(ctx context.Context, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, error error) {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	// 如果投票任期小，直接返回false
	rep := &pb.RequestVoteReply{}
	if rf.CurrentTerm > args.CurrentTerm {
		rep.State = -1
		rep.Term = rf.CurrentTerm
		return rep, nil
	}
	// 判断日志是否比投票人新（大于等于）
	// 当前节点没有Log

	var lastLogTerm int64 = 0
	if len(rf.Log) != 0 {
		lastLogTerm = rf.Log[len(rf.Log)-1].Term
	}

	if args.LastLogIndex < int64(len(rf.Log)) || args.LastLogTerm < lastLogTerm {
		return
	}
	// 类型为投票
	if args.State == 0 {
		//节点状态如果为candidate，变为follower，同步信息
		if rf.State == 1 {
			rf.State = 0
			rf.Leader = false
			rf.CurrentTerm = args.CurrentTerm
			rf.Leader_pos = args.Pos
			rep.State = 1
		}
		//如果消息为Leader广播，且当前节点Leader_pos或者任期号未更新
	} else if args.State == 1 {
		rf.State = 0
		rf.Leader = false
		rf.CurrentTerm = args.CurrentTerm
		rf.Leader_pos = args.Pos
	}
	rf.Persist()
	return rep, nil
}

func (rf *RaftService) init() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.State = 1
	rf.Leader = false
	rf.Leader_pos = -1
	rf.CurrentTerm += 1
	rf.Persist()
}

// 心跳检测
func (rf *RaftService) heartBeat() {
	for rf.Leader {
		for peer := 0; peer < len(rf.Peers); peer++ {
			go func(p int) {
				reply, _ := rf.sendAppendEntries(p, &pb.AppendEntriesArgs{Term: rf.CurrentTerm})
				if reply == nil {
					return
				}
				if rf.CurrentTerm < reply.Term {
					rf.Mu.Lock()
					rf.Leader = false
					rf.State = 1
					rf.CurrentTerm = reply.Term
					rf.Persist()
					rf.Mu.Unlock()
				}

			}(peer)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *RaftService) Snapshot(){
	for {
		if len(rf.Log)>1e3{
			rf.Mu.Lock()
			sp:=&snapshot.Snapshot{}
			data,_:=db.Db.Get(util.StringToByte(common.RaftSnapshot),nil)
			json.Unmarshal(data,&sp)
			sp.LastIndex+=int64(len(rf.Log))
			sp.LastTerm=rf.Log[len(rf.Log)-1].Term
			bytedata,_:=json.Marshal(sp)
			db.Db.Put(util.StringToByte(common.RaftSnapshot),bytedata,nil)
			rf.Log=rf.Log[0:0]
			rf.Mu.Unlock()
		}
		time.Sleep(1*time.Millisecond)
	}

}

// 应用到状态机
func (rf *RaftService) StateMachine() {
	for rf.Dead != 1 {
		if rf.LastApplied < rf.CommitIndex {
			//rf.applyCh <- ApplyMsg{CommandIndex: rf.LastApplied + 1, Command: rf.Log[rf.LastApplied].Command,
			//	CommandValid: true}
			db.Db.Put([]byte(rf.Log[rf.LastApplied].Command.Key), rf.Log[rf.LastApplied].Command.Value, nil)
			rf.LastApplied++
		}
		time.Sleep(3 * time.Millisecond)
	}
}



// 调用rpc追加日志
func (rf *RaftService) sendAppendEntries(server int, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, error error) {
	conn, err := grpc.Dial(rf.Peers[server], grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	c := pb.NewRaftClient(conn)
	res, err := c.AppendEntries(context.Background(), args)
	return res, err
}

// 调用rpc投票
func (rf *RaftService) sendRequestVote(server int, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, error error) {
	conn, err := grpc.Dial(rf.Peers[server], grpc.WithInsecure())
	if err != nil {
		log.Println(err)
	}
	defer conn.Close()
	c := pb.NewRaftClient(conn)
	res, err := c.RequestVote(context.Background(), args)
	if err != nil {
		log.Println(err)
	}
	return res, err
}

// 广播日志
func (rf *RaftService) AppendLogs(res *conf.Res) {
	var ans int32 = 0

	for peer := 0; peer < len(rf.Peers); peer++ {
		if int64(peer) == rf.Me {
			continue
		}
		if !rf.Leader{
			return
		}
		//prevLogIndex 为 NextIndex的值
		go func(peer int, retry int) {
			for {
				var term int64
				if !rf.Leader {
					break
				}
				if rf.NextIndex[peer] <= 0 {
					term = -1
				} else {
					term = rf.Log[rf.NextIndex[peer]-1].Term
				}
				var arg = &pb.AppendEntriesArgs{Term: rf.CurrentTerm, LeaderPos: rf.Me,
					PrevLogIndex: rf.NextIndex[peer] - 1, PrevLogTerm: term,
					Entries: rf.Log[rf.NextIndex[peer]:], CommitIndex: rf.CommitIndex}

				// 调用日志追加rpc
				reply, _ := rf.sendAppendEntries(peer, arg)
				if reply == nil {
					log.Println(fmt.Sprintf("%d AppendEntries is nil", peer))
					continue
				}

				if reply.Success {
					// 日志匹配成功
					rf.Mu.Lock()
					rf.MatchIndex[peer] = rf.NextIndex[peer]
					atomic.AddInt32(&ans, 1)
					rf.Mu.Unlock()
					break
				} else {
					if rf.CurrentTerm < reply.Term {
						rf.Mu.Lock()
						rf.Leader = false
						rf.State = 1
						rf.CurrentTerm = reply.Term
						rf.Persist()
						rf.Mu.Unlock()
						break
					} else if rf.NextIndex[peer] == 0 {
						fmt.Printf("当前位置 %d,term %d,日志 %d leader %v\n", rf.Me, rf.CurrentTerm, rf.CommitIndex, rf.Leader)
					} else {
						rf.NextIndex[peer]--
					}
				}
			}
		}(peer, 0)
	}
	// 如果超过一半的follower同步成功，提交日志

	go func() {
		oks := 1
		for {
			if (oks == 1 && ans > (int32)(len(rf.Peers)/2)) || (oks != 1 && int32(oks) != ans) {
				rf.Mu.Lock()
				rf.CommitIndex = int64(len(rf.Log))
				rf.Mu.Unlock()
				// 二阶段发出提交日志
				for peer := 0; peer < len(rf.Peers); peer++ {
					go func(peer int) {
						rf.Mu.Lock()
						rf.NextIndex[peer] = int64(len(rf.Log))
						rf.MatchIndex[peer] = rf.NextIndex[peer]
						rf.Mu.Unlock()
						args := &pb.AppendEntriesArgs{Type: 1, Term: rf.CurrentTerm}
						rf.sendAppendEntries(peer, args)
					}(peer)
				}
				oks = int(ans)
				if ans == (int32)(len(rf.Peers)) {
					return
				}
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

}

func (rf *RaftService) Persist() {
	data, _ := json.Marshal(rf)
	db.Db.Put(util.StringToByte(common.RaftPersist), data, nil)
	log.Println("raft persist")
}

func (rf *RaftService) ReadPersist() {
	rafts := &RaftService{}
	data, err := db.Db.Get(util.StringToByte(common.RaftPersist), nil)
	if err!=nil{
		json.Unmarshal(data, &rafts)
		log.Println("raft read persist")
		rf.CurrentTerm=rafts.CurrentTerm
		rf.Leader_pos=rafts.Leader_pos
		rf.Log=rafts.Log
	}else{
		sp:=&snapshot.Snapshot{LastIndex: 0,LastTerm: 0}
		data,_:=json.Marshal(sp)
		db.Db.Put(util.StringToByte(common.RaftSnapshot),data,nil)
		Rf.Leader_pos = -1
	}

}
