package ring

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"

	"github.com/pixperk/plethora/node"
	"github.com/pixperk/plethora/types"
	"github.com/pixperk/plethora/vclock"
)

type Ring struct {
	Nodes      []*node.Node
	NodeMap    map[string]*node.Node // nodeID -> node for O(1) lookup
	Partitions []Partition
	Q          int // total number of partitions, fixed
	//N is the replication factor, we walk clockwise from a node and pick up n nodes to build up
	//preference list for the node, to image the rights
	N int
	//quorum
	R int //min number of reads
	W int //min number of writes
}

// creates a ring with Q equal-sized partitions distributed evenly across the given nodes.
func NewRing(q int, n int, R int, W int, nodes []*node.Node) (*Ring, error) {
	s := len(nodes)
	if s == 0 {
		return nil, fmt.Errorf("need at least one node")
	}
	if q%s != 0 {
		return nil, fmt.Errorf("Q (%d) must be divisible by number of nodes (%d)", q, s)
	}

	if R > n {
		return nil, fmt.Errorf("R (%d) cannot be greater than N (%d)", R, n)
	}
	if W > n {
		return nil, fmt.Errorf("W (%d) cannot be greater than N (%d)", W, n)
	}
	if R+W <= n {
		return nil, fmt.Errorf("R + W (%d) must be greater than N (%d) for quorum", R+W, n)
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
		N:          n,
		R:          R,
		W:          W,
	}, nil
}

// Lookup hashes a key and returns the node that owns the partition it falls into.
// hash(key) % Q -> partition index -> token -> owner node
func (r *Ring) Lookup(key string) *node.Node {
	hash := md5Hash(key)
	partitionID := int(hash % uint64(r.Q))
	ownerID := r.Partitions[partitionID].Token.NodeID
	node, exists := r.NodeMap[ownerID]
	if !exists {
		return nil // should never happen since partitions are pre-initialized
	}
	return node
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

	found := false
	// remove from Nodes slice
	for i, n := range r.Nodes {
		if n.NodeID == nodeID {
			r.Nodes = append(r.Nodes[:i], r.Nodes[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("node with ID %s not found", nodeID)
	}

	// reassign partitions to maintain Q/S partitions per node
	for i := 0; i < r.Q; i++ {
		r.Partitions[i].Token.NodeID = r.Nodes[i%s].NodeID
	}

	return nil
}

// PreferenceList walks the ring clockwise from the key's partition
// and collects N distinct nodes responsible for storing this key.
func (r *Ring) PreferenceList(key types.Key) []*node.Node {
	hash := md5Hash(string(key))
	partitionID := int(hash % uint64(r.Q))

	nodes := make([]*node.Node, 0, r.N)
	//track if node is seen
	seen := make(map[string]bool)

	// walk partitions clockwise, skip duplicate nodes
	// make sure to stop after we've seen N distinct nodes, or we've looped through all partitions (in case N > number of nodes)
	for i := 0; i < r.Q && len(nodes) < r.N; i++ {
		idx := (partitionID + i) % r.Q
		nodeID := r.Partitions[idx].Token.NodeID
		if seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		nodes = append(nodes, r.NodeMap[nodeID])
	}

	return nodes
}

// Get reads from N nodes in the preference list, requires at least R responses.
// Deduplicates using vector clocks and returns only causally distinct versions.
func (r *Ring) Get(key types.Key) ([]types.Value, error) {
	nodes := r.PreferenceList(key)
	var allVals []types.Value
	responses := 0
	for _, n := range nodes {
		vals, ok := n.Get(key)
		if ok {
			allVals = append(allVals, vals...)
		}
		responses++
	}
	//quorum : if we got responses from fewer than R nodes, we consider the read failed due to insufficient replicas responding. With networking, this would be based on timeouts and error handling, but in this in-memory simulation, we assume all calls succeed.
	if responses < r.R {
		return nil, fmt.Errorf("quorum not met: got %d responses, need R=%d", responses, r.R)
	}
	if len(allVals) == 0 {
		return nil, nil
	}
	return dedup(allVals), nil
}

// dedup filters a list of values down to only causally distinct versions.
// Drops any value that is an ancestor of another value in the list.
func dedup(vals []types.Value) []types.Value {
	var result []types.Value
	for i, v := range vals {
		dominated := false
		for j, other := range vals {
			if i == j {
				continue
			}
			// if other descends from v (and they're not equal), v is an ancestor, drop it
			if other.Clock.Descends(v.Clock) && !v.Clock.Descends(other.Clock) {
				dominated = true
				break
			}
			// if they have identical clocks, keep only the first occurrence
			if other.Clock.Descends(v.Clock) && v.Clock.Descends(other.Clock) && j < i {
				dominated = true
				break
			}
		}
		if !dominated {
			result = append(result, v)
		}
	}
	return result
}

// Put writes to N nodes in the preference list, requires at least W successes.
// The first node (coordinator) builds the value with the vector clock.
func (r *Ring) Put(key types.Key, val string, ctx vclock.VClock) error {
	nodes := r.PreferenceList(key)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available")
	}

	// coordinator builds the clock and value
	coordinator := nodes[0]
	var clock vclock.VClock
	if ctx == nil {
		clock = vclock.NewVClock()
	} else {
		clock = ctx.Copy()
	}
	clock.Increment(coordinator.NodeID)

	value := types.Value{
		Data:  val,
		Clock: clock,
	}

	// write to all N nodes via storage directly (bypass node.Put to avoid double-incrementing the clock)
	acks := 0
	for _, n := range nodes {
		n.Storage.Put(key, value)
		acks++ // in-process calls always succeed; with networking this would be conditional
	}
	if acks < r.W {
		return fmt.Errorf("quorum not met: got %d acks, need W=%d", acks, r.W)
	}
	return nil
}
