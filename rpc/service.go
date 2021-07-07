package rpc

import (
	"confact1/conf"
	"confact1/db"
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
	// 2. 类型为Leader二阶段提交
	if args.Type == 1 {
		rf.CommitIndex = int64(len(rf.Log))
		return rep, nil
	}
	// 心跳检测，直接返回
	if len(args.Entries) == 0 {
		if args.Term < rf.CurrentTerm {
			rep.Term = rf.CurrentTerm
			rep.Success = false
			return rep, nil
		}
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
		return rep, nil
	}
	// 1. 领导人版本小直接返回false
	if args.Term < rf.CurrentTerm {
		rep.Term = rf.CurrentTerm
		rep.Success = false
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
	if len(rf.Log) == 0 {
		if args.LastLogIndex < -1 || args.LastLogTerm < -1 {
			return
		}
	} else {
		// 当前节点最后一个日志的下标或者任期大于选举的节点日志，直接返回false
		if args.LastLogIndex < rf.Log[len(rf.Log)-1].Index || args.LastLogTerm < rf.Log[len(rf.Log)-1].Term {
			return
		}
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
	return rep, nil
}

func (rf *RaftService) init() {
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	rf.State = 1
	rf.Leader = false
	rf.Leader_pos = -1
	rf.CurrentTerm += 1
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
					rf.Mu.Unlock()
				}

			}(peer)
		}
		time.Sleep(50 * time.Millisecond)
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
		//prevLogIndex 为 NextIndex的值
		//retry 重试7次
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
				// fmt.Println(rf.CurrentTerm, rf.me, rf.NextIndex[peer], term, rf.CommitIndex)
				var arg = &pb.AppendEntriesArgs{Term: rf.CurrentTerm, LeaderPos: rf.Me,
					PrevLogIndex: rf.NextIndex[peer] - 1, PrevLogTerm: term,
					Entries: rf.Log[rf.NextIndex[peer]:], CommitIndex: rf.CommitIndex}
				if rf.NextIndex[peer] == 0 {
					fmt.Println(peer, rf.NextIndex[peer], arg.PrevLogIndex, arg.PrevLogTerm)
				}

				reply, _ := rf.sendAppendEntries(peer, arg)
				if rf.NextIndex[peer] == 0 {
					fmt.Println(reply)
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
						rf.Mu.Unlock()
						break
					} else if rf.NextIndex[peer] == 0 {
						fmt.Println("??--??")
					} else {
						rf.NextIndex[peer]--
					}
				}
				retry++
				if retry >= 7 {
					break
				}
			}
		}(peer, 0)
	}
	// 如果超过一半的follower同步成功，提交日志

	//time.Sleep(50 * time.Millisecond)
	go func() {
		for {
			if ans >= (int32)(len(rf.Peers)/2) {
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
				break
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()

}
