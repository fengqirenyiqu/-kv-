package client

import (
	"client/config"
	"client/kvStore"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu         sync.Mutex
	Servers    []*config.ClientEnd
	GrpcClient []kvStore.KvStoreServiceClient
	// You will have to modify this struct.
	// 只需要确保clientId每次重启不重复，那么clientId+seqId就是安全的
	clientId int64 // 客户端唯一标识，存储程序的server端需要区别不同client端的seqId，
	seqId    int64 // 该客户端单调递增的请求id，重发的请求
	leaderId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk() *Clerk {
	ck := new(Clerk)
	// You'll have to add code here.
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := kvStore.GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	leaderId := ck.currentLeader()
	for {
		//发送失败就会重复发送
		if reply, err := ck.GrpcClient[leaderId].Get(context.Background(), &args); err == nil {
			if reply.Err == "OK" { // 命中
				return reply.Value
			} else if reply.Err == "ErrNoKey" { // 不存在
				return ""
			}
		} else {
			fmt.Println(err.Error())
		}
		leaderId = ck.changeLeader() //到这说明发送失败，说明leader可能变了
		time.Sleep(1 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := kvStore.PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	leaderId := ck.currentLeader()
	for {
		if reply, err := ck.GrpcClient[leaderId].PutAppend(context.Background(), &args); err == nil {
			if reply.Err == "OK" { // 成功
				break
			}
		} else {
			fmt.Println(err.Error())
		}
		leaderId = ck.changeLeader()
		time.Sleep(1 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
func (ck *Clerk) currentLeader() (leaderId int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leaderId = ck.leaderId
	return
}

func (ck *Clerk) changeLeader() (leaderId int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.leaderId = (ck.leaderId + int64(1)) % int64(len(ck.Servers))
	return ck.leaderId
}
