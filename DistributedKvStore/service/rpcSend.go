package service

import (
	"DistributedKvStore/rpc"
	"context"
	"fmt"
	"google.golang.org/grpc"
)

//var kacp = keepalive.ClientParameters{
//	Time:                60 * time.Millisecond,  // send pings every 10 seconds if there is no activity
//	Timeout:             120 * time.Millisecond, // wait 1 second for ping ack before considering the connection dead
//	PermitWithoutStream: true,                   // send pings even without active streams
//}

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
	c := rf.MakeGrpcClient(server)
	reply, err = c.RequestVote(context.TODO(), args)
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
			rf.persist()
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
	c := rf.MakeGrpcClient(server)
	res, err := c.AppendEntries(context.TODO(), args) //reply不能直接等于返回值，grpc调用会返回一个新的对象地址，会覆盖掉原来的
	if err != nil {
		//fmt.Printf("发给 %v 的AppendEntries  err %v\n", server, err.Error())
		return false
	}
	reply.Success = res.Success
	reply.Term = res.Term
	//fmt.Printf("send %v\n", reply.Success)
	return true

}
func (rf *Raft) MakeGrpcClient(server int) rpc.RaftServiceClient {
	var err error = nil
	rf.Peers[server].Conn, err = grpc.Dial(rf.Peers[server].Ip, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err.Error())
	}
	client := rpc.NewRaftServiceClient(rf.Peers[server].Conn)

	return client
}
