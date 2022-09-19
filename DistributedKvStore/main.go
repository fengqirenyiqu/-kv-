package main

import (
	"DistributedKvStore/config"
	"DistributedKvStore/kvStore"
	"DistributedKvStore/rpc"
	"DistributedKvStore/service"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"net"
	"os"
	"strconv"
	"time"
)

var kaep = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second, // If a client pings more than once every 5 seconds, terminate the connection
	PermitWithoutStream: true,            // Allow pings even when there are no active streams
}

var kasp = keepalive.ServerParameters{
	MaxConnectionIdle:     15 * time.Second, // If a client is idle for 15 seconds, send a GOAWAY
	MaxConnectionAge:      30 * time.Second, // If any connection is alive for more than 30 seconds, send a GOAWAY
	MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
	Time:                  5 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
	Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
}

func main() {
	//1初始化grpc对线
	rpcServer := grpc.NewServer()
	//2注册服务
	persist := &service.Persister{}
	clientEnd := config.InitClientEnd()
	if len(os.Args) < 2 {
		return
	}
	me, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println(err.Error())
	}
	kv := kvStore.StartKVServer(clientEnd, int64(me), persist, 5)
	//注册rpc服务的时候注意不要new一个 新的，要用Start跟Make生成的。否则都是raft 0
	kvStore.RegisterKvStoreServiceServer(rpcServer, kv)
	rpc.RegisterRaftServiceServer(rpcServer, kv.Rf)

	//listen
	listener, err := net.Listen("tcp", clientEnd[me].Ip)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer listener.Close()
	//4绑定服务
	rpcServer.Serve(listener)
}
