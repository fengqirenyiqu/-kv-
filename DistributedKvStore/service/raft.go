package service

import (
	"DistributedKvStore/config"
	"DistributedKvStore/rpc"
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

func (rf *Raft) GetState() (int64, bool) {

	// Your code here (2A).
	return rf.CurrentTerm, rf.Isleader
}
func (rf *Raft) Start(op *rpc.Op) (int64, int64, bool) {
	index := int64(-1)
	term := int64(-1)
	isLeader := false
	//TODO killed补充
	//if rf.killed() == true {
	//	return index, term, isLeader
	//}
	if !rf.Isleader {
		return index, term, isLeader
	}
	isLeader = true
	// Your code here (2B).
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	op.Term = rf.CurrentTerm
	fmt.Printf("   raft %v,收到了一条命令 %v\n", rf.Me, op)
	rf.Logs = append(rf.Logs, op)
	index = int64(len(rf.Logs))
	term = rf.CurrentTerm
	//fmt.Printf("    raft:%v Term:%v 收到了命令,本地日志index更新为%v \n", rf.Me, rf.CurrentTerm, index-1)
	//for Server, _ := range rf.peers {
	//	rf.nextIndex[Server] = index - 1
	//}
	rf.persist()
	return index, term, isLeader
}
func Make(peers []*config.ClientEnd, me int64,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Peers = peers
	rf.Persister = persister
	//TODO 修改peers配置
	rf.Peers = peers
	rf.Me = me
	rf.GrpcClient = make([]rpc.RaftServiceClient, len(rf.Peers))
	//rf.InitGrpcClient()
	//--------------------------------------------------------
	// Your initialization code here (2A, 2B, 2C).
	rf.VotedFor = int64(-1)
	rf.Overtime = time.Duration(rand.Intn(150)+500) * time.Millisecond
	rf.Election_timeout = time.NewTimer(rf.Overtime)
	rf.Heartbeat_interval = time.NewTimer(120 * time.Millisecond)
	//2-B
	rf.ApplyChan = applyCh
	rf.Logs = make([]*rpc.Op, 0)
	rf.NextIndex = make([]int64, len(peers))
	rf.MatchIndex = make([]int64, len(peers))
	go rf.ticker()
	//go rf.StateMachine()
	// initialize from State persisted before a crasht
	rf.readPersist(persister.readPersist(rf.Me))
	return rf
}

func (rf *Raft) ticker() {
	//TODO 修改循环终止
	for rf.Dead != int64(1) {
		if !rf.Isleader {
			select {
			case <-rf.Election_timeout.C: //candidate处理
				if rf.State == 1 {
					//说明选举超时,应该重新发起选举
					rf.Mu.Lock()
					//rf.VotedFor = -1 ,不该重置VotedFor,该轮任期确确实实的投过票了
					rf.State = 0
					rf.Mu.Unlock()
					rf.reset_election_timeout()
					continue
				}
				rf.Candidate()
			default:
				time.Sleep(2 * time.Millisecond)
			}
		} else {
			select {
			case <-rf.Heartbeat_interval.C:
				rf.broadcastAppendEntries()
			default:
				time.Sleep(2 * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) Candidate() {
	rf.Mu.Lock()
	//选举开始也需要重置计时器
	rf.reset_election_timeout()
	rf.CurrentTerm++
	rf.State = int64(1)
	fmt.Printf("    raft:%v Term:%v  become candidate  \n", rf.Me, rf.CurrentTerm)
	rf.VotedFor = rf.Me
	rf.persist()
	voteNumber := int64(1)
	//TODO
	//2-B中优化投票规则
	for id, _ := range rf.Peers {
		if int64(id) == rf.Me {
			continue
		}
		args := &rpc.RequestVoteArgs{
			Term:        rf.CurrentTerm,
			CandidateId: rf.Me,
		}
		if len(rf.Logs)-1 >= 0 {
			args.LastLogIndex = int64(len(rf.Logs) - 1)
			args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
		} else {
			args.LastLogIndex = -1
			args.LastLogTerm = -1
		}
		rep := &rpc.RequestVoteReply{}
		go rf.sendRequestVote(id, args, rep, &voteNumber, rf.CurrentTerm) //不能一个goroutine内对所有成员发送rpc,会阻塞
		//不能给每个rpc传同一个参数,否则可能会发现明明没有给他投票,另一边却收到了投票.
		//异步处理选举请求,要特别注意投票结果的处理
		//之后的sendRequestVote中,由于锁的存在,是一个个的发出投票请求
		//最后的voteNUmber总会达到最大值,只需要对这个值进行判断,就能知道是否获得了足够的投票
	}
	rf.Mu.Unlock()
}

func (rf *Raft) broadcastAppendEntries() {
	var ans int32 = 1
	rf.Mu.Lock()
	//不是leader,就不要发了
	//if !rf.isleader {
	//	rf.Mu.Unlock()
	//	return
	//}
	index := int64(len(rf.Logs))
	for id, _ := range rf.Peers {
		if int64(id) == rf.Me {
			continue
		}
		go func(server int) {
		LOOP:
			for { //循环到日志匹配成功为止
				//加入不加这个,当某个leader在某个goroutine中降级了更新term,其他goroutine还是会继续.
				rf.Mu.Lock()
				if !rf.Isleader {
					rf.Mu.Unlock()
					break
				}
				//matchIndex已经更新了了,但是这一轮还有人的日志没发完
				if rf.MatchIndex[server] > index {
					index = rf.MatchIndex[server]
				}
				agrs := &rpc.AppendEntriesArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.Me,
					LogEntry:          rf.Logs[rf.MatchIndex[server]:index],
					PrevLogIndex:      rf.MatchIndex[server] - 1,
					LeaderCommitIndex: rf.CommitIndex,
				}
				if rf.MatchIndex[server] <= 0 {
					agrs.PrevLogTerm = -1
				} else {
					agrs.PrevLogTerm = rf.Logs[rf.MatchIndex[server]-1].Term
				}
				reply := &rpc.AppendEntriesReply{}
				rf.Mu.Unlock()
				ok := rf.sendAppendEntries(server, agrs, reply)
				if ok {
					//不能这样写,这里的rpc返回值不可能为nil
					//if reply == nil {
					//	//fmt.Println("没有命令,没有日志需要补发")
					//	break //continue?
					//}
					//if reply.Term == -1 {
					//	break
					//}
					//fmt.Printf("  broad %v\n", reply.Success)
					if reply.Success {
						if reply.Term == -1 {
							//fmt.Println("break")
							break LOOP
						}
						//日志匹配成功
						rf.Mu.Lock()
						//fmt.Printf("    raft:%v Term:%v  发给%v的日志匹配成功 \n", rf.Me, rf.CurrentTerm, server)
						atomic.AddInt32(&ans, 1)
						if ans > (int32)(len(rf.Peers)/2) {
							//fmt.Println("日志提交成功")
							//tmp := rf.nextIndex[server]
							rf.NextIndex[server] = int64(len(rf.Logs)) // 为什么是这个,加入没有命令进来,那下次entry就是[1:],并不会越界,len(entry)=0,并且一旦有命令进来entry不为0
							//加强提交条件,原因可以看figure8,
							rf.MatchIndex[server] = rf.NextIndex[server]
							if rf.Logs[index-1].Term == rf.CurrentTerm {
								rf.CommitIndex = index
								rf.StateMachine()
							}
							// 困惑的点,还剩下部分follower怎么办,
							//答:假如后续有新的命令进来,剩下的follower虽然会复制最新的日志,但是只会提交到被提交到的位置,
							ans = -1 //保证幂等性,
						}
						rf.Mu.Unlock()
						break
					} else {
						//返回false只有两张可能 1是因为对方的term比自己大,自己降级
						//2是因为日志匹配不上
						if rf.CurrentTerm < reply.Term {
							rf.Mu.Lock()
							rf.CurrentTerm = reply.Term
							rf.Isleader = false
							rf.VotedFor = -1
							rf.State = 0
							rf.reset_election_timeout()
							rf.persist()
							rf.Mu.Unlock()
							break
						} else {
							rf.Mu.Lock()
							if rf.MatchIndex[server] != 0 {
								rf.MatchIndex[server]--
							}
							rf.Mu.Unlock()
						}
					}
				} else {
					break
				}
			}
		}(id)
	}
	rf.Heartbeat_interval = time.NewTimer(120 * time.Millisecond)
	rf.Mu.Unlock()
}
func (rf *Raft) Kill() {
	atomic.StoreInt64(&rf.Dead, 1)
	// Your code here, if desired.
	rf.Mu.Lock()
	rf.Election_timeout.Stop()
	rf.Heartbeat_interval.Stop()
	rf.Mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt64(&rf.Dead)
	return z == 1
}

func (rf *Raft) persist() {
	path := fmt.Sprintf("./raftPersist%v", rf.Me) //单服务器测试可以这样
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
	defer f.Close()
	var n int64
	if err != nil {
		fmt.Println(err.Error())
	} else {
		// offset
		//os.Truncate(filename, 0) //clear
		n, _ = f.Seek(0, os.SEEK_END)
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.Logs)
	e.Encode(rf.VotedFor)
	e.Encode(rf.CurrentTerm)
	data := w.Bytes()
	f.WriteAt(data, n)
}

func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewReader(data)
	var votedFor int64
	logs := []*rpc.Op{}
	var currentTerm int64
	d := gob.NewDecoder(r)
	d.Decode(&votedFor)
	d.Decode(&logs)
	d.Decode(&currentTerm)
	rf.Logs = logs
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
}
