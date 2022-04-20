package rpc

import (
	"confact1/conf"
	pb "confact1/confact/proto"
	"confact1/logs"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var RaftMap sync.Map

func Start(runConfig *conf.RaftInfo) {
	Rf := &RaftService{}
	Rf.ReadPersist()
	go Rf.SnapshotInit()
	go Rf.Snapshot()
	Rf.Peers = runConfig.RaftList
	Rf.Me = runConfig.RaftID
	Rf.LastApplied = 0
	Rf.CommitIndex = 0
	// 为什么是multi-raft，因为数组长度不够
	Rf.MatchIndex = make([]int64, len(conf.JsonConf.RaftsRPC))
	Rf.NextIndex = make([]int64, len(conf.JsonConf.RaftsRPC))
	Rf.Random = rand.New(rand.NewSource(time.Now().UnixNano() + Rf.Me))
	Rf.Leader = false
	clock := Rf.Random.Intn(500)
	clock += 500
	Rf.ElectionTimer = time.NewTimer(time.Duration(clock * 1e6))
	// 开启状态机
	go Rf.StateMachine()

	// 加入raftMap
	RaftMap.Store(runConfig.RaftID, Rf)

	//超时计数器
	var wg sync.WaitGroup
	go func() {
		for Rf.Dead != 1 {
			//初始化
			<-Rf.ElectionTimer.C
			Rf.init()
			logs.PrintInfo(Rf.Me, "当前Term ", Rf.CurrentTerm)
			if Rf.Dead == 1 {
				break
			}
			var voteNum int64 = 1
			for _, i := range Rf.Peers {
				if i == Rf.Me {
					continue
				}
				wg.Add(1)
				// 判断Log日志是否为空
				args := pb.RequestVoteArgs{CurrentTerm: Rf.CurrentTerm,
					LastLogIndex: Rf.GetLastIndex(), LastLogTerm: Rf.GetLastTerm()}
				go func(pos int64) {
					//发起投票
					reply, _ := Rf.sendRequestVote(int64(pos), &args)
					if reply == nil {
						wg.Done()
						return
					}
					if reply.State == -1 {
						//如果自己任期小，要更新
						Rf.Mu.Lock()
						Rf.CurrentTerm = reply.Term
						Rf.Leader = false
						Rf.State = 1
						Rf.Persist()
						Rf.Mu.Unlock()
						wg.Done()
						return
					}
					// 接受投票
					if reply.State == 1 {
						logs.PrintInfo(Rf.Me, "对方raftID: ", pos, "同意该投票")
						atomic.AddInt64(&voteNum, 1)
					}
					if voteNum > int64(len(Rf.Peers)/2) && !Rf.Leader {
						//选举成功，更新信息
						//TODO 如果自己不是candidate而是给别人投票了变成follower怎么办
						Rf.Mu.Lock()
						logs.PrintInfo(Rf.Me, "voteNum ", voteNum)
						logs.PrintInfo(Rf.Me, "成为Leader Term： ", Rf.CurrentTerm)
						Rf.Leader = true
						Rf.Leader_pos = Rf.Me
						Rf.State = 2
						Rf.Persist()
						for i := 0; i < len(Rf.Peers); i++ {
							Rf.NextIndex[i] = int64(len(Rf.Log))
							Rf.MatchIndex[i] = 0
						}
						Rf.Mu.Unlock()
						// 广播Leader消息
						for _, j := range Rf.Peers {
							if j == Rf.Me {
								continue
							}
							go Rf.sendRequestVote(j, &pb.RequestVoteArgs{State: 1, Pos: Rf.Me, CurrentTerm: Rf.CurrentTerm})
						}
						//维持心跳
						go Rf.heartBeat()
					}
					wg.Done()
				}(i)
			}
			//等待所有的消息返回
			wg.Wait()
			clock := Rf.Random.Intn(500)
			clock += 500
			Rf.ElectionTimer.Reset(time.Duration(clock * 1e6))
		}
	}()
}
