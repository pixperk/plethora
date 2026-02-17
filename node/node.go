package node

import (
	"github.com/pixperk/plethora/storage"
	"github.com/pixperk/plethora/types"
)

type Node struct {
	NodeID  string
	Storage *storage.Storage
}

func NewNode(nodeID string) *Node {
	return &Node{
		NodeID:  nodeID,
		Storage: storage.NewStorage(),
	}
}

func (n *Node) Get(key types.Key) ([]types.Value, bool) {
	return n.Storage.Get(key)
}

func (n *Node) Put(key types.Key, val string) {
	//construct the value with the context
	ctx := types.Context{
		Version: 1, //initial version
		NodeID:  n.NodeID,
	}

	value := types.Value{
		Data:    val,
		Context: ctx,
	}

	n.Storage.Put(key, value)
}
