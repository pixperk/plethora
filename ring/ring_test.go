package ring

import (
	"fmt"
	"testing"

	"github.com/pixperk/plethora/node"
)

func makeNodes(ids ...string) []*node.Node {
	nodes := make([]*node.Node, len(ids))
	for i, id := range ids {
		nodes[i] = node.NewNode(id, fmt.Sprintf("localhost:%d", 5001+i))
	}
	return nodes
}

func ownerCount(r *Ring, nodeID string) int {
	count := 0
	for _, p := range r.Partitions {
		if p.Token.NodeID == nodeID {
			count++
		}
	}
	return count
}

// helper: N=3, R=2, W=2 (common Dynamo config)
func newTestRing(t *testing.T, q int, nodes []*node.Node) *Ring {
	t.Helper()
	r, err := NewRing(q, 3, 2, 2, nodes)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestNewRingDistribution(t *testing.T) {
	r := newTestRing(t, 6, makeNodes("n1", "n2", "n3"))

	for _, id := range []string{"n1", "n2", "n3"} {
		if c := ownerCount(r, id); c != 2 {
			t.Fatalf("node %s owns %d partitions, want 2", id, c)
		}
	}
}

func TestNewRingRejectsZeroNodes(t *testing.T) {
	_, err := NewRing(6, 3, 2, 2, []*node.Node{})
	if err == nil {
		t.Fatal("expected error for zero nodes")
	}
}

func TestNewRingRejectsIndivisibleQ(t *testing.T) {
	_, err := NewRing(7, 3, 2, 2, makeNodes("n1", "n2", "n3"))
	if err == nil {
		t.Fatal("expected error when Q not divisible by S")
	}
}

func TestNewRingRejectsInvalidQuorum(t *testing.T) {
	// R + W must be > N
	_, err := NewRing(12, 3, 1, 1, makeNodes("n1", "n2", "n3", "n4"))
	if err == nil {
		t.Fatal("expected error when R + W <= N")
	}
}

func TestLookupDeterministic(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	first := r.Lookup("user:42")
	for i := 0; i < 100; i++ {
		if got := r.Lookup("user:42"); got.NodeID != first.NodeID {
			t.Fatalf("lookup not deterministic: got %s, want %s", got.NodeID, first.NodeID)
		}
	}
}

func TestLookupReturnsValidNode(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

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
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	err := r.AddNode(node.NewNode("n4", "localhost:5004"))
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(r.Nodes))
	}

	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if c := ownerCount(r, id); c != 3 {
			t.Fatalf("node %s owns %d partitions, want 3", id, c)
		}
	}
}

func TestAddNodeRejectsIndivisibleQ(t *testing.T) {
	r := newTestRing(t, 6, makeNodes("n1", "n2", "n3"))

	err := r.AddNode(node.NewNode("n4", "localhost:5004"))
	if err == nil {
		t.Fatal("expected error when Q not divisible by new S")
	}

	if len(r.Nodes) != 3 {
		t.Fatalf("ring mutated on failed add: got %d nodes", len(r.Nodes))
	}
}

func TestRemoveNode(t *testing.T) {
	r, _ := NewRing(12, 3, 2, 2, makeNodes("n1", "n2", "n3", "n4"))

	err := r.RemoveNode("n4")
	if err != nil {
		t.Fatal(err)
	}

	if len(r.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(r.Nodes))
	}

	for _, id := range []string{"n1", "n2", "n3"} {
		if c := ownerCount(r, id); c != 4 {
			t.Fatalf("node %s owns %d partitions, want 4", id, c)
		}
	}

	if c := ownerCount(r, "n4"); c != 0 {
		t.Fatalf("removed node n4 still owns %d partitions", c)
	}
}

func TestRemoveNodeRejectsLastNode(t *testing.T) {
	r, _ := NewRing(6, 1, 1, 1, makeNodes("n1"))
	err := r.RemoveNode("n1")
	if err == nil {
		t.Fatal("expected error when removing last node")
	}
}

func TestRemoveNodeNotFound(t *testing.T) {
	r := newTestRing(t, 6, makeNodes("n1", "n2", "n3"))
	err := r.RemoveNode("n99")
	if err == nil {
		t.Fatal("expected error for non-existent node")
	}
}

func TestPreferenceListReturnsNDistinctNodes(t *testing.T) {
	r, _ := NewRing(12, 3, 2, 2, makeNodes("n1", "n2", "n3", "n4"))

	plist := r.PreferenceList("user:1")
	if len(plist) != 3 {
		t.Fatalf("expected 3 nodes in preference list, got %d", len(plist))
	}

	seen := make(map[string]bool)
	for _, n := range plist {
		if seen[n.NodeID] {
			t.Fatalf("duplicate node %s in preference list", n.NodeID)
		}
		seen[n.NodeID] = true
	}
}

func TestPreferenceListFirstNodeIsOwner(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	owner := r.Lookup("user:1")
	plist := r.PreferenceList("user:1")

	if plist[0].NodeID != owner.NodeID {
		t.Fatalf("first node in preference list (%s) != owner (%s)", plist[0].NodeID, owner.NodeID)
	}
}

func TestPutGet(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	if err := r.Put("user:1", "alice", nil); err != nil {
		t.Fatal(err)
	}
	vals, err := r.Get("user:1")
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 || vals[0].Data != "alice" {
		t.Fatalf("expected [alice], got %v", vals)
	}
}

func TestGetMissing(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	vals, err := r.Get("nope")
	if err != nil {
		t.Fatal(err)
	}
	if vals != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestPutReplicatesToNNodes(t *testing.T) {
	r, _ := NewRing(12, 3, 2, 2, makeNodes("n1", "n2", "n3", "n4"))

	r.Put("user:1", "alice", nil)

	plist := r.PreferenceList("user:1")

	for _, n := range plist {
		vals, ok := n.Get("user:1")
		if !ok || len(vals) == 0 {
			t.Fatalf("node %s in preference list missing the value", n.NodeID)
		}
		if vals[0].Data != "alice" {
			t.Fatalf("node %s has wrong data: %s", n.NodeID, vals[0].Data)
		}
	}

	prefSet := make(map[string]bool)
	for _, n := range plist {
		prefSet[n.NodeID] = true
	}
	for _, n := range r.Nodes {
		if prefSet[n.NodeID] {
			continue
		}
		_, ok := n.Get("user:1")
		if ok {
			t.Fatalf("non-preference node %s has the value", n.NodeID)
		}
	}
}

func TestPutOverwriteVersions(t *testing.T) {
	r := newTestRing(t, 12, makeNodes("n1", "n2", "n3"))

	r.Put("k", "v1", nil)

	vals, _ := r.Get("k")
	ctx := vals[0].Clock

	r.Put("k", "v2", ctx)

	vals, _ = r.Get("k")
	if len(vals) != 1 || vals[0].Data != "v2" {
		t.Fatalf("expected [v2], got %v", vals)
	}
}
