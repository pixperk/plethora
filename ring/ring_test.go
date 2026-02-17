package ring

import (
	"testing"

	"github.com/pixperk/plethora/node"
)

func makeNodes(ids ...string) []*node.Node {
	nodes := make([]*node.Node, len(ids))
	for i, id := range ids {
		nodes[i] = node.NewNode(id)
	}
	return nodes
}

// count how many partitions a node owns
func ownerCount(r *Ring, nodeID string) int {
	count := 0
	for _, p := range r.Partitions {
		if p.Token.NodeID == nodeID {
			count++
		}
	}
	return count
}

func TestNewRingDistribution(t *testing.T) {
	r, err := NewRing(6, makeNodes("n1", "n2", "n3"))
	if err != nil {
		t.Fatal(err)
	}

	// each node should own Q/S = 6/3 = 2 partitions
	for _, id := range []string{"n1", "n2", "n3"} {
		if c := ownerCount(r, id); c != 2 {
			t.Fatalf("node %s owns %d partitions, want 2", id, c)
		}
	}
}

func TestNewRingRejectsZeroNodes(t *testing.T) {
	_, err := NewRing(6, []*node.Node{})
	if err == nil {
		t.Fatal("expected error for zero nodes")
	}
}

func TestNewRingRejectsIndivisibleQ(t *testing.T) {
	_, err := NewRing(7, makeNodes("n1", "n2", "n3"))
	if err == nil {
		t.Fatal("expected error when Q not divisible by S")
	}
}

func TestLookupDeterministic(t *testing.T) {
	r, _ := NewRing(12, makeNodes("n1", "n2", "n3"))

	// same key always maps to same node
	first := r.Lookup("user:42")
	for i := 0; i < 100; i++ {
		if got := r.Lookup("user:42"); got.NodeID != first.NodeID {
			t.Fatalf("lookup not deterministic: got %s, want %s", got.NodeID, first.NodeID)
		}
	}
}

func TestLookupReturnsValidNode(t *testing.T) {
	nodes := makeNodes("n1", "n2", "n3")
	r, _ := NewRing(12, nodes)

	nodeIDs := map[string]bool{"n1": true, "n2": true, "n3": true}
	keys := []string{"a", "b", "c", "foo", "bar", "user:1", "user:999"}
	for _, key := range keys {
		n := r.Lookup(key)
		if n == nil {
			t.Fatalf("Lookup(%q) returned nil", key)
		}
		if !nodeIDs[n.NodeID] {
			t.Fatalf("Lookup(%q) returned unknown node %s", key, n.NodeID)
		}
	}
}

func TestAddNode(t *testing.T) {
	r, _ := NewRing(12, makeNodes("n1", "n2", "n3"))

	err := r.AddNode(node.NewNode("n4"))
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(r.Nodes))
	}

	// each node should own Q/S = 12/4 = 3 partitions
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if c := ownerCount(r, id); c != 3 {
			t.Fatalf("node %s owns %d partitions, want 3", id, c)
		}
	}
}

func TestAddNodeRejectsIndivisibleQ(t *testing.T) {
	r, _ := NewRing(6, makeNodes("n1", "n2", "n3"))

	// 6 % 4 != 0
	err := r.AddNode(node.NewNode("n4"))
	if err == nil {
		t.Fatal("expected error when Q not divisible by new S")
	}

	// ring should be unchanged
	if len(r.Nodes) != 3 {
		t.Fatalf("ring mutated on failed add: got %d nodes", len(r.Nodes))
	}
}

func TestRemoveNode(t *testing.T) {
	r, _ := NewRing(12, makeNodes("n1", "n2", "n3", "n4"))

	err := r.RemoveNode("n4")
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(r.Nodes))
	}

	// each node should own Q/S = 12/3 = 4 partitions
	for _, id := range []string{"n1", "n2", "n3"} {
		if c := ownerCount(r, id); c != 4 {
			t.Fatalf("node %s owns %d partitions, want 4", id, c)
		}
	}

	// removed node should own nothing
	if c := ownerCount(r, "n4"); c != 0 {
		t.Fatalf("removed node n4 still owns %d partitions", c)
	}
}

func TestRemoveNodeRejectsLastNode(t *testing.T) {
	r, _ := NewRing(6, makeNodes("n1"))
	err := r.RemoveNode("n1")
	if err == nil {
		t.Fatal("expected error when removing last node")
	}
}

func TestRemoveNodeNotFound(t *testing.T) {
	r, _ := NewRing(6, makeNodes("n1", "n2", "n3"))
	err := r.RemoveNode("n99")
	if err == nil {
		t.Fatal("expected error for non-existent node")
	}
}
