package service

import (
	"fmt"
	"io/ioutil"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	raftstate []byte
	snapshot  []byte
}

func (ps *Persister) readPersist(me int64) []byte {
	path := fmt.Sprintf("./raftPersist%v", me)
	result, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println(err.Error())
	}
	return result
}
