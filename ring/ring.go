package ring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"

	"github.com/pixperk/plethora/node"
)

type Ring struct {
	Nodes      []*node.Node
	NodeMap    map[string]*node.Node // nodeID -> node for O(1) lookup
	Partitions []Partition
	Q          int // total number of partitions, fixed
}

// creates a ring with Q equal-sized partitions distributed evenly across the given nodes.
func NewRing(q int, nodes []*node.Node) (*Ring, error) {
	s := len(nodes)
	if s == 0 {
		return nil, fmt.Errorf("need at least one node")
	}
	if q%s != 0 {
		return nil, fmt.Errorf("Q (%d) must be divisible by number of nodes (%d)", q, s)
	}

	partitions := make([]Partition, q)
	for i := 0; i < q; i++ {
		partitions[i] = Partition{
			ID: i,
			// round-robin: partition i is assigned to node i%S, giving each node Q/S partitions
			Token: Token{NodeID: nodes[i%s].NodeID},
		}
	}

	nodeMap := make(map[string]*node.Node, s)
	for _, n := range nodes {
		nodeMap[n.NodeID] = n
	}

	return &Ring{
		Nodes:      nodes,
		NodeMap:    nodeMap,
		Partitions: partitions,
		Q:          q,
	}, nil
}

// Lookup hashes a key and returns the node that owns the partition it falls into.
// hash(key) % Q -> partition index -> token -> owner node
func (r *Ring) Lookup(key string) *node.Node {
	hash := md5Hash(key)
	partitionID := int(hash % uint64(r.Q))
	ownerID := r.Partitions[partitionID].Token.NodeID
	return r.NodeMap[ownerID]
}

// md5Hash returns the first 8 bytes of the MD5 digest as a uint64.
func md5Hash(key string) uint64 {
	sum := md5.Sum([]byte(key))
	return binary.BigEndian.Uint64(sum[:8])
}

func (r *Ring) AddNode(newNode *node.Node) error {
	// validate before mutating
	s := len(r.Nodes) + 1
	if r.Q%s != 0 {
		return fmt.Errorf("Q (%d) must be divisible by number of nodes (%d)", r.Q, s)
	}

	r.Nodes = append(r.Nodes, newNode)
	r.NodeMap[newNode.NodeID] = newNode

	// reassign partitions to maintain Q/S partitions per node
	for i := 0; i < r.Q; i++ {
		r.Partitions[i].Token.NodeID = r.Nodes[i%s].NodeID
	}

	return nil
}

func (r *Ring) RemoveNode(nodeID string) error {
	// validate before mutating
	s := len(r.Nodes) - 1
	if s == 0 {
		return fmt.Errorf("cannot remove last node")
	}
	if r.Q%s != 0 {
		return fmt.Errorf("Q (%d) must be divisible by number of nodes (%d)", r.Q, s)
	}

	delete(r.NodeMap, nodeID)

	// remove from Nodes slice
	for i, n := range r.Nodes {
		if n.NodeID == nodeID {
			r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
			break
		}
	}

	// reassign partitions to maintain Q/S partitions per node
	for i := 0; i < r.Q; i++ {
		r.Partitions[i].Token.NodeID = r.Nodes[i%s].NodeID
	}

	return nil
}
