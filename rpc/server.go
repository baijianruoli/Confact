package rpc

import (
	tc "confact1/conf"
	"fmt"
	pb "github.com/baijianruoli/conf/confact/proto"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var Rf *RaftService

func Start() {
	Rf = &RaftService{}
	fmt.Println(tc.Conf)
	Rf.Peers = tc.Conf.Peers
	Rf.Me = tc.Conf.Me
	Rf.LastApplied = 0
	Rf.CommitIndex = 0
	Rf.Leader_pos = -1
	Rf.Random = rand.New(rand.NewSource(time.Now().UnixNano() + Rf.Me))
	Rf.Leader = false
	clock := Rf.Random.Intn(500)
	clock += 500
	Rf.ElectionTimer = time.NewTimer(time.Duration(clock * 1e6))
	// 开启状态机
	go Rf.StateMachine()
	//超时计数器
	var wg sync.WaitGroup
	go func() {
		for Rf.Dead != 1 {
			//初始化
			<-Rf.ElectionTimer.C
			Rf.init()
			if Rf.Dead == 1 {
				break
			}
			var voteNum int64
			for i := 0; i < len(Rf.Peers); i++ {
				if i == (int)(Rf.Me) {
					continue
				}
				wg.Add(1)
				// 判断Log日志是否为空
				var args = pb.RequestVoteArgs{}
				if len(Rf.Log) == 0 {
					args = pb.RequestVoteArgs{CurrentTerm: Rf.CurrentTerm,
						LastLogIndex: -1, LastLogTerm: -1}
				} else {
					args = pb.RequestVoteArgs{CurrentTerm: Rf.CurrentTerm,
						LastLogIndex: Rf.Log[len(Rf.Log)-1].Index, LastLogTerm: Rf.Log[len(Rf.Log)-1].Term}
				}
				go func(pos int) {
					//发起投票
					reply, _ := Rf.sendRequestVote(pos, &args)
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
						Rf.Mu.Unlock()
						wg.Done()
						return
					}
					// 接受投票
					if reply.State == 1 {
						atomic.AddInt64(&voteNum, 1)
					}
					if voteNum >= int64(len(Rf.Peers)/2) && !Rf.Leader {
						//选举成功，更新信息
						//TODO 如果自己不是candidate而是给别人投票了变成follower怎么办
						Rf.Mu.Lock()
						log.Println(Rf.Me, "成为Leader", Rf.CurrentTerm)
						Rf.Leader = true
						Rf.Leader_pos = Rf.Me
						Rf.State = 2
						for i := 0; i < len(Rf.Peers); i++ {
							Rf.NextIndex[i] = int64(len(Rf.Log))
							Rf.MatchIndex[i] = 0
						}
						Rf.Mu.Unlock()
						// 广播Leader消息
						for j := 0; j < len(Rf.Peers); j++ {
							if j == int(Rf.Me) {
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
