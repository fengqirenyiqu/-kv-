package kvStore

import (
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

type DbPersist struct {
	db *leveldb.DB
}

//转换
type SliceMock struct {
	addr uintptr
	len  int
	cap  int
}

func Initleveldb(me int64) *leveldb.DB {
	path := fmt.Sprintf("./test%v.db", me) //单服务器测试可以这样
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		fmt.Println(err.Error())
	}
	return db
}
func (kv *KVServer) DbGet(key string) (string, bool) {
	data, err := kv.db.Get([]byte(key), nil)
	if err != nil {
		return string(data), false
	}
	return string(data), true
}
func (kv *KVServer) DbPutAppend(key string, value string) error {
	err := kv.db.Put([]byte(key), []byte(value), nil)
	return err
}
