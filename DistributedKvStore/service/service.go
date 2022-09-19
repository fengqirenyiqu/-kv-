package service

import (
	"DistributedKvStore/config"
	"DistributedKvStore/rpc"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      *rpc.Op //这里不能用interface 因为proto协议并不支持interface{}类型，示例代码用interface{},最终也是用*Op断言获得传过去的Op的值
	CommandIndex int64
}
type Raft struct {
	Mu         sync.Mutex          // Lock to protect shared access to this peer's State
	Peers      []*config.ClientEnd // RPC end points of all peers
	GrpcClient []rpc.RaftServiceClient
	Persister  *Persister // Object to hold thIs peer's persisted State
	Me         int64      // this peer's index into peers[]
	Dead       int64      // set by Kill()

	// Your data here (2A, 2B, 2C).
	Overtime           time.Duration
	Isleader           bool
	State              int64 //0表示follower,1表示candidate
	CurrentTerm        int64
	Election_timeout   *time.Timer //选举超时间隔
	Heartbeat_interval *time.Timer //心跳间隔
	VotedFor           int64       // 给谁投票了,都没投用-1表示
	Logs               []*rpc.Op
	NextIndex          []int64 //记录每个日志的同下一个索引,
	MatchIndex         []int64 //记录每个fallow日志最大索引，0递增
	CommitIndex        int64   //已提交的最大编号,比如Commit了下标为13的的日志,则为14
	LastApplied        int64   //上次已提交的下标,
	ApplyChan          chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// State a Raft server Must maintain.
	*rpc.UnimplementedRaftServiceServer
}

func (rf *Raft) RequestVote(ctx context.Context, args *rpc.RequestVoteArgs) (*rpc.RequestVoteReply, error) {
	// Your code here (2A, 2B).
	reply := &rpc.RequestVoteReply{}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	//旧leader收到了投票请求的处理,降级后参与投票
	//reply.Term = rf.CurrentTerm
	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return reply, nil
	}
	if rf.Isleader && rf.CurrentTerm < args.Term {
		rf.Isleader = false
		rf.VotedFor = -1
		rf.State = 0
		rf.CurrentTerm = args.Term
		//fmt.Println("降级")
		//rf.persist()
		rf.reset_election_timeout()
		//rf.election_timeout = time.NewTimer(rf.overtime * time.Millisecond)
	}
	//这里不需要考虑同一个term下或者旧candidate,因为只有正确的candidate才能让follower投票,否则VotedFor不为-1,他们也拿不到票
	if rf.CurrentTerm < args.Term {
		rf.VotedFor = -1
		rf.State = 0
		rf.CurrentTerm = args.Term
		//rf.persist()
		//rf.election_timeout = time.NewTimer(rf.overtime * time.Millisecond)
	}
	reply.Term = rf.CurrentTerm

	//正常逻辑follower收到投票请求处理
	//TODO 优化投票规则
	lastLogIndex := int64(-1)
	lastLogTerm := int64(-1)
	if (len(rf.Logs) - 1) >= 0 {
		lastLogIndex = int64(len(rf.Logs) - 1)
		lastLogTerm = rf.Logs[lastLogIndex].Term
	}
	//第二个条件适用于超时重传,比如candidate收不到信息,重复发送了投票请求
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		//投票成功,重新计时,投票失败不需要重新计时
		//2-B带日志信息的复杂选举规则
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			rf.VotedFor = args.CandidateId
			fmt.Printf("    raft:%v Term:%v  投票给了%v 我的日志长度:%v,lastLogTerm:%v,lastLogIndex:%v ,Args.LastLogTerm:%v,Agrs.LastLogIndex:%v\n", rf.Me, rf.CurrentTerm, rf.VotedFor, len(rf.Logs), lastLogTerm, lastLogIndex, args.LastLogTerm, args.LastLogIndex)
			reply.VoteGranted = true
			//reply.Term = rf.CurrentTerm
			rf.reset_election_timeout() //给别人投票,重置投票时间
			//rf.persist()
			return reply, nil
		}
	}
	return reply, nil

}

func (rf *Raft) AppendEntries(ctx context.Context, args *rpc.AppendEntriesArgs) (*rpc.AppendEntriesReply, error) {
	//fmt.Printf("    raft:%v Term:%v 收到了raft %v的Ap信息 args.Term %v \n", rf.Me, rf.CurrentTerm, args.LeaderId, args.Term)

	//根据args.Entry是否为空判断是心跳建立(还有检测)信息,还是日志复制信息
	reply := &rpc.AppendEntriesReply{}
	rf.Mu.Lock()
	defer rf.Mu.Unlock()
	//leader任期比自己小,直接返回fasle{
	if rf.CurrentTerm > args.Term {
		//来自旧leader的心跳信息或者日志信息
		//fmt.Println("旧leader的信息")
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return reply, nil
	}
	//旧leader收到
	if rf.Isleader && rf.CurrentTerm < args.Term {
		//每个都需要独立判断Isleader是因为我没有用一个statues来表示follower,candidate跟leader.
		rf.Isleader = false
		rf.State = 0
		rf.reset_election_timeout()
		//fmt.Printf("    raft:%v Term:%v 收到了新leader:%v,旧节点(或者旧leader)的Ap自动降级,term更新为:%v  \n", rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
	}
	//任期比自己大,说明自己所在任期落后.更改任期,因为这个任期没参与投票所以重置VotedFor
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.State = 0
		//rf.persist()
	}
	//假如自己是同期的candidate,自己更新状态为follower
	if rf.State == 1 {
		rf.State = 0
	}
	rf.reset_election_timeout()

	//TODO
	//因为是统一截断,所以为了防止旧的日志信息覆盖掉新的,需要某种策略,比如对当前长度跟prevlogindex进行比对,加入时间戳等,或者优化日志复制逻辑.没有冲突就不截断..
	if args.PrevLogIndex > int64(len(rf.Logs)-1) {
		reply.Success = false
		return reply, nil
	}
	if args.PrevLogIndex != -1 {
		if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return reply, nil
		}
	}
	//

	//注意当没有数据的时候,比如entry :=[]int{} ,其中entry[0:]本地调用不为nil,但是在这里的rpc中会因为没有数据,在传过来的时候变为nil.
	//由于切片特性,比如有一个命令进来并且提交了之后,正常情况下entry[1:0],下标1并没有数据,不会提示越界.但是没有数据,当有新的命令进来,这时候就有数据了
	//然后因为上面的判断,当有信息说明有新的命令进来,添加,或者前面比对失败开始日志匹配,这样也肯定会携带信息.
	if len(args.LogEntry) == 0 {
		//fmt.Println("收到了心跳")
		//心跳信息,或者没有附带日志信息.return
		rf.CurrentTerm = args.Term
		rf.State = 0    //讲candidate恢复为follower
		reply.Term = -1 //说明这是心跳信息返回
	} else {
		rf.Logs = rf.Logs[:args.PrevLogIndex+1]
		for _, v := range args.LogEntry {
			rf.Logs = append(rf.Logs, v)
		}
		//fmt.Printf("    raft:%v Term:%v 复制了日志,更新为%v \n", rf.Me, rf.CurrentTerm, rf.Logs[len(rf.Logs)-1])
	}
	//把比对上之后,后面的给清空,
	reply.Success = true
	//fmt.Printf("    raft:%v Term:%v 返回了reply.Success %v,reply.Term %v \n", rf.Me, rf.CurrentTerm, reply.Success, reply.Term)
	//更新CommitIndex
	if args.LeaderCommitIndex > int64(len(rf.Logs)) {
		rf.CommitIndex = int64(len(rf.Logs))
	} else {
		rf.CommitIndex = args.LeaderCommitIndex
	}
	//rf.CommitIndex = args.LeaderCommitIndex
	rf.StateMachine()

	//fmt.Printf("    raft:%v Term:%v 复制了日志,更新为%v \n", rf.me, rf.CurrentTerm, len(rf.logs)-1)
	//rf.persist()
	////fmt.Printf("    raft:%v Term:%v 持久化了日志\n", rf.me, rf.CurrentTerm)
	return reply, nil
}

func (rf *Raft) reset_election_timeout() {
	rf.Overtime = time.Duration(rand.Intn(150)+500) * time.Millisecond
	rf.Election_timeout = time.NewTimer(rf.Overtime)
}
func (rf *Raft) StateMachine() {
	for rf.LastApplied < rf.CommitIndex {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[rf.LastApplied],
			CommandIndex: rf.LastApplied + 1, //+1是因为测试希望元素是从1开始的(即第一次调用start返回1),但是我们这里CommitIndex表示len(),lastApplied表示下标
		}
		//fmt.Printf("    raft:%v Term:%v  提交了日志,下标为%v  \n", rf.me, rf.currentTerm, rf.LastApplied)
		rf.LastApplied++
		rf.ApplyChan <- applyMsg
	}
}
