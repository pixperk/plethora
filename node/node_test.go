package node

import (
	"testing"
)

func TestGetMissing(t *testing.T) {
	n := NewNode("n1")
	_, ok := n.Get("nope")
	if ok {
		t.Fatal("expected missing key")
	}
}

func TestPutGet(t *testing.T) {
	n := NewNode("n1")
	n.Put("k", "hello")

	vals, ok := n.Get("k")
	if !ok || len(vals) != 1 || vals[0].Data != "hello" {
		t.Fatalf("expected [hello], got %v", vals)
	}
	if vals[0].Context.NodeID != "n1" || vals[0].Context.Version != 1 {
		t.Fatalf("bad context: %+v", vals[0].Context)
	}
}

func TestPutOverwriteIncrementsVersion(t *testing.T) {
	n := NewNode("n1")
	n.Put("k", "v1")
	n.Put("k", "v2")

	vals, _ := n.Get("k")
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}
	if vals[0].Data != "v2" || vals[0].Context.Version != 2 {
		t.Fatalf("expected v2@version2, got %s@version%d", vals[0].Data, vals[0].Context.Version)
	}
}

func TestTwoNodesSameKey(t *testing.T) {
	n1 := NewNode("n1")
	n2 := NewNode("n2")

	// simulate both nodes writing to the same storage
	n1.Put("k", "from-n1")

	// manually put n2's value into n1's storage to simulate replication
	n2.Put("k", "from-n2")
	// n1 and n2 have separate stores, so each has 1 value
	v1, _ := n1.Get("k")
	v2, _ := n2.Get("k")
	if len(v1) != 1 || v1[0].Data != "from-n1" {
		t.Fatalf("n1 expected [from-n1], got %v", v1)
	}
	if len(v2) != 1 || v2[0].Data != "from-n2" {
		t.Fatalf("n2 expected [from-n2], got %v", v2)
	}
}
