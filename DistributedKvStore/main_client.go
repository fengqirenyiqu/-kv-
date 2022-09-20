package main

import (
	"DistributedKvStore/client"
	"DistributedKvStore/config"
	"DistributedKvStore/kvStore"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1 创建客户端连接
	var err error
	clientEnd := config.InitClientEnd()
	clerk := client.MakeClerk()
	clerk.Servers = clientEnd
	clerk.GrpcClient = make([]kvStore.KvStoreServiceClient, len(clerk.Servers))
	for i, _ := range clerk.Servers {
		clerk.Servers[i].Conn, err = grpc.Dial(clerk.Servers[i].Ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Println("grpc Dial err: ", err)
			return
		}
		clerk.GrpcClient[i] = kvStore.NewKvStoreServiceClient(clerk.Servers[i].Conn)
	}
	clerk.PutAppend("test", "6666666666666", "Put")
	fmt.Println(clerk.Get("test"))
	defer func() {
		for i, _ := range clientEnd {
			clientEnd[i].Conn.Close()
		}
	}()
}
