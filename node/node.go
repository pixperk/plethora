package node

import (
	"github.com/pixperk/plethora/storage"
	"github.com/pixperk/plethora/types"
	"github.com/pixperk/plethora/vclock"
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

// Put accepts a context clock from a previous Get (nil for fresh writes).
// It copies the clock, increments this node's entry, and writes to storage.
func (n *Node) Put(key types.Key, val string, ctx vclock.VClock) {
	var clock vclock.VClock
	if ctx == nil {
		clock = vclock.NewVClock()
	} else {
		clock = ctx.Copy()
	}
	clock.Increment(n.NodeID)

	value := types.Value{
		Data:  val,
		Clock: clock,
	}

	n.Storage.Put(key, value)
}
