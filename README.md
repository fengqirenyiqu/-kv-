# 分布式kv存储系统

1 共识算法 Raft
2 grpc的使用
3 leveldb的简易使用(准备替换成自己实现的KV存储程序，基于LSM tree)

## 如何使用 

1 确定你已经导入了grpc所需插件以及库（开启gomod，如需关闭，请把下列库放到$gopath/src下）

```
go get -u google.golang.org/grpc/cmd/protoc-gen-go-grpc
go get -u google.golang.org/protobuf/cmd/protoc-gen-go
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

2编译proto文件

```
cd到rpc
protoc --go_out=./ --go-grpc_out=./ *.proto
cd到kvStore
protoc --go_out=./ --go-grpc_out=./ *.proto
```

3 如何部署

config目录为配置项，可以添加任意个节点，如需部署到不同服务器，请修改kvStore/leveldb.go中Initleveldb 函数中的path变量为自己的数据存储目录

```
cd到主目录 启动
go run main.go ${serverId}  //serverId节点数0~n-1
```

4调用api 复制client目录到你的目录下（里面的kvStore目录为编译好的kvStore文件，如有异常请复制DistributedKvStore/kvStore下的kvStore.proto文件到client目录下，再自行编译）

```
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
	
	clerk.PutAppend("test", "6666666666666", "Put")//测试 
	fmt.Println(clerk.Get("test"))
	defer func() {
		for i, _ := range clientEnd {
			clientEnd[i].Conn.Close()
		}
	}()
}
```

## 后续改进

1优化raftlog 添加日志压缩（lab3B运行不够完美所以先完善一下在加入快照）

2添加分布式事务，实现方案为percolator事务(学习ing)
