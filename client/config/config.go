package config

import "google.golang.org/grpc"

//TODO
//从etcd读取编号 ，读完+1 现在这里只有三个，往后可以优化 从etcd动态加入删除节点
type ClientEnd struct {
	Ip   string
	Conn *grpc.ClientConn
}

func InitClientEnd() []*ClientEnd {
	c1 := &ClientEnd{
		Ip: "127.0.0.1:8088",
	}
	c2 := &ClientEnd{
		Ip: "127.0.0.1:8089",
	}
	c3 := &ClientEnd{
		Ip: "127.0.0.1:8090",
	}

	return []*ClientEnd{c1, c2, c3}
}
