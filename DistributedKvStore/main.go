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
