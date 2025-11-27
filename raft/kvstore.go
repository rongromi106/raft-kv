package raft

import (
	"encoding/json"
	"fmt"
	"sync"
)

type KVStore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

type KVOpType string

const (
	OpPut    KVOpType = "put"
	OpDelete KVOpType = "delete"
)

type KVCommand struct {
	Op    KVOpType `json:"op"` // "put" / "delete"
	Key   string   `json:"key"`
	Value string   `json:"value,omitempty"` // delete 时可空
}

func (kv *KVStore) Apply(command []byte) error {
	var cmd KVCommand
	if err := json.Unmarshal(command, &cmd); err != nil {
		return err
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(kv.data, cmd.Key)
	default:
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}

	return nil
}
