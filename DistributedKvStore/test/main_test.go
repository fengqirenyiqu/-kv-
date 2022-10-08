package test

import (
	"DistributedKvStore/client"
	"DistributedKvStore/config"
	"DistributedKvStore/rpc"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io/ioutil"
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
	db, err := leveldb.OpenFile("../test1.db", nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	data, err := db.Get([]byte("okokokok"), nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(string(data))
}

func TestPersist(t *testing.T) {
	path := fmt.Sprintf("../raftPersist%v", 2) //单服务器测试可以这样
	result, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println(err.Error())
	}
	r := bytes.NewReader(result)
	var votedFor int64
	logs := []*rpc.Op{}
	var currentTerm int64
	d := gob.NewDecoder(r)
	d.Decode(&logs)
	d.Decode(&votedFor)
	d.Decode(&currentTerm)
	fmt.Println(votedFor, logs, currentTerm)
}
