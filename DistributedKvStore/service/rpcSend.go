package service

import (
	"DistributedKvStore/rpc"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (rf *Raft) sendRequestVote(server int, args *rpc.RequestVoteArgs, reply *rpc.RequestVoteReply, voteNumber *int64, currentTerm int64) bool {
	//TODO
	//更新args用于2-B中日志模块加入后的投票规则
	rf.Mu.Lock()
	if rf.State == 0 || rf.Isleader || currentTerm != rf.CurrentTerm {
		//旧candidate更新自己状态后,不应该发送剩余的没法出去的投票,已经成为了leader,过期投票
		rf.Mu.Unlock()
		return false
	}
	rf.Mu.Unlock()
	var err error = nil
	var ok bool
	fmt.Printf("    raft %v 发给 %v 的投票请求\n", rf.Me, server)
	//grpc调用的返回值是一个新的*RequestVoteReply;会覆盖掉传过来的reply的地址,但是这里不需要管，Ap那里需要
	reply, err = rf.GrpcClient[server].RequestVote(context.TODO(), args)
	if err != nil {
		//fmt.Printf("发给 %v 的投票请求  err %v", server, err.Error())
		return ok
	}
	ok = true
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.State = 0
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.reset_election_timeout()
			//TODO 持久化raft数据
			//rf.persist()
			return false
		}
		//拒绝延迟投票,延迟投票分别就是返回的term比当前的小,上面的过期投票判断无法彻底顾虑,因为调用rpc不加锁所以可能返回的慢导致term又变了.
		if reply.Term < rf.CurrentTerm {
			return false
		}
		if reply.VoteGranted {
			//请求投票成功则+1
			*voteNumber += 1
			//fmt.Printf("    raft:%v 收到来自%v的投票 \n", rf.me, server)
		}
	}

	if *voteNumber >= int64(((len(rf.Peers) / 2) + 1)) {
		rf.Isleader = true
		//state变换必须在重置计时器之后
		//rf.reset_election_timeout()
		rf.State = 2
		fmt.Printf("    raft:%v Term:%v  become leader 得票数%v ,这是发给%v的goroutine\n", rf.Me, rf.CurrentTerm, *voteNumber, server)
		*voteNumber = -1 //成为leader之后,重置,避免二次执行
		//加了过不了figure8 不可靠测试
		for i, _ := range rf.Peers {
			if int64(i) == rf.Me {
				continue
			}
			rf.NextIndex[i] = int64(len(rf.Logs))
		}
	}
	return true
}

func (rf *Raft) sendAppendEntries(server int, args *rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) bool {
	var err error = nil
	res, err := rf.GrpcClient[server].AppendEntries(context.TODO(), args) //reply不能直接等于返回值，grpc调用会返回一个新的对象地址，会覆盖掉原来的
	if err != nil {
		fmt.Printf("发给 %v 的AppendEntries  err %v", server, err.Error())
		return false
	}
	reply.Success = res.Success
	reply.Term = res.Term
	//fmt.Printf("send %v\n", reply.Success)
	return true

}
func (rf *Raft) InitGrpcClient() {
	var err error = nil
	rf.GrpcClient = make([]rpc.RaftServiceClient, len(rf.Peers))
	for i, _ := range rf.Peers {
		rf.Peers[i].Conn, err = grpc.Dial(rf.Peers[i].Ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println(err.Error())
		}
		rf.GrpcClient[i] = rpc.NewRaftServiceClient(rf.Peers[i].Conn)
	}
}
