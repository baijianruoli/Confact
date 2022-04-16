package rpc

import (
	"confact1/common"
	pb "confact1/confact/proto"
	"confact1/db"
	"confact1/util"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type RaftService struct {
	Mu            sync.Mutex // Lock to protect shared access to this peer's State
	Peers         []int64    // RPC end point64s of all Peers 需要持久化
	Me            int64      // this peer's index int64o Peers[]
	Dead          int64
	State         int64
	CurrentTerm   int64
	Leader        bool
	Leader_pos    int64
	Random        *rand.Rand
	ElectionTimer *time.Timer `json:"-"`
	BatchTimer    *time.Timer `json:"-"`
	Log           []*pb.LogEntry

	CommitIndex int64 //已提交的最大编号
	LastApplied int64
	NextIndex   []int64
	MatchIndex  []int64
	GrpcClient  sync.Map
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
	if args.PrevLogIndex <= 0 {
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
		//if index == -1 {
		//	rep.Success = false
		//	return rep, nil
		//}
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

	lastLogTerm := rf.GetLastTerm()
	lastLogIndex := rf.GetLastIndex()

	if args.LastLogIndex < lastLogIndex || args.LastLogTerm < lastLogTerm {
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
				reply, _ := rf.sendAppendEntries(int64(p), &pb.AppendEntriesArgs{Term: rf.CurrentTerm})
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

// 生成快照
// 生成策略 1000一次
func (rf *RaftService) Snapshot() {
	for {
		if rf.LastApplied > 1e3 && rf.LastApplied == rf.CommitIndex {
			rf.Mu.Lock()
			sp := &Snapshot{}
			data, _ := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), nil)
			json.Unmarshal(data, &sp)
			sp.LastIndex += int64(len(rf.Log))
			sp.LastTerm = rf.Log[len(rf.Log)-1].Term
			bytedata, _ := json.Marshal(sp)
			log.Println(rf.LastApplied, "生成快照")
			db.Db.Put(util.StringToByte(fmt.Sprintf("%s%d", common.RaftSnapshot, rf.Me)), bytedata, nil)
			// 初始化
			rf.Log = rf.Log[0:0]
			rf.LastApplied = 0
			rf.CommitIndex = 0
			for _, i := range rf.Peers {
				rf.MatchIndex[i] = 0
				rf.NextIndex[i] = 0
			}
			rf.Persist()
			rf.Mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}

}

// 应用到状态机
func (rf *RaftService) StateMachine() {
	for rf.Dead != 1 {
		if rf.LastApplied < rf.CommitIndex {
			rf.Mu.Lock()
			go db.LevelDBSaveLogs(rf.Log[rf.LastApplied])
			rf.LastApplied++
			rf.Mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
	}
}

// 广播日志
func (rf *RaftService) AppendLogs() {
	var ans int32 = 0

	for _, peer := range rf.Peers {
		if peer == rf.Me {
			continue
		}
		if !rf.Leader {
			return
		}
		//prevLogIndex 为 NextIndex的值
		go func(peer int64, retry int) {
			for {
				var term int64
				if !rf.Leader {
					break
				}
				if rf.MatchIndex[peer] <= 0 {
					term = -1
				} else {
					term = rf.Log[rf.MatchIndex[peer]-1].Term
				}
				var arg = &pb.AppendEntriesArgs{Term: rf.CurrentTerm, LeaderPos: rf.Me,
					PrevLogIndex: rf.MatchIndex[peer] - 1, PrevLogTerm: term,
					Entries: rf.Log[rf.MatchIndex[peer]:], CommitIndex: rf.CommitIndex}

				// 调用日志追加rpc
				fmt.Printf("%d 发送日志 起始位置%d 终止位置%d\n", rf.Me, rf.MatchIndex[peer]-1, rf.CommitIndex)
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
				for _, peer := range rf.Peers {
					go func(peer int64) {
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

// 持久化
func (rf *RaftService) Persist() {
	go func() {
		data, err := json.Marshal(rf)
		if err != nil {
			log.Printf("raft persist error %s", err.Error())
			return
		}
		db.Db.Put(util.StringToByte(fmt.Sprintf("%s%d", common.RaftPersist, rf.Me)), data, nil)
		log.Println("raft persist")
	}()
}

// 读取持久化
func (rf *RaftService) ReadPersist() {
	rafts := &RaftService{}
	data, err := db.Db.Get(util.StringToByte(fmt.Sprintf("%s%d", common.RaftPersist, rf.Me)), nil)
	if err == nil {
		json.Unmarshal(data, &rafts)
		log.Println("raft read persist")
		rf.CurrentTerm = rafts.CurrentTerm
		rf.Leader_pos = rafts.Leader_pos
		rf.Log = rafts.Log
		rf.Leader = rafts.Leader
	} else {
		rf.Persist()
		rf.Leader_pos = -1
	}

}
