package main

import (
	"DistributedKvStore/client"
	"DistributedKvStore/config"
	"DistributedKvStore/rpc"
	"context"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
)

func TestRequestVote(t *testing.T) {
	// 1 创建客户端连接
	clientEnd := config.InitClientEnd()
	clerk := client.MakeClerk()
	clerk.Servers = clientEnd
	conn, err := grpc.Dial(clientEnd[1].Ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err.Error())
	}
	grpcClient := rpc.NewRaftServiceClient(conn)
	args := rpc.RequestVoteArgs{Term: 9, CandidateId: 0, LastLogTerm: 1, LastLogIndex: 1}
	reply, err := grpcClient.RequestVote(context.Background(), &args)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(reply.VoteGranted, 55)
}
func TestAppendEntries(t *testing.T) {
	// 1 创建客户端连接
	clientEnd := config.InitClientEnd()
	clerk := client.MakeClerk()
	clerk.Servers = clientEnd
	conn, err := grpc.Dial(clientEnd[1].Ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err.Error())
	}
	grpcClient := rpc.NewRaftServiceClient(conn)
	args := rpc.AppendEntriesArgs{
		Term:     1,
		LeaderId: 0,
	}
	reply, err := grpcClient.AppendEntries(context.Background(), &args)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(reply.Success, 55)
}
func TestDbSave(t *testing.T) {
	db, err := leveldb.OpenFile("./test0.db", nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	data, err := db.Get([]byte("test"), nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(string(data))
}
