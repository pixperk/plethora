package storage

import (
	"testing"

	"github.com/pixperk/plethora/types"
)

func TestGetMissing(t *testing.T) {
	s := NewStorage()
	_, ok := s.Get("nope")
	if ok {
		t.Fatal("expected missing key")
	}
}

func TestPutGet(t *testing.T) {
	s := NewStorage()
	s.Put("k", types.Value{Data: "v", Context: types.Context{Version: 1, NodeID: "n1"}})

	vals, ok := s.Get("k")
	if !ok || len(vals) != 1 || vals[0].Data != "v" {
		t.Fatalf("expected [v], got %v", vals)
	}
}

func TestPutSameNodeIncrementsVersion(t *testing.T) {
	s := NewStorage()
	s.Put("k", types.Value{Data: "v1", Context: types.Context{Version: 1, NodeID: "n1"}})
	s.Put("k", types.Value{Data: "v2", Context: types.Context{Version: 1, NodeID: "n1"}})

	vals, _ := s.Get("k")
	if len(vals) != 1 {
		t.Fatalf("expected 1 value, got %d", len(vals))
	}
	if vals[0].Data != "v2" || vals[0].Context.Version != 2 {
		t.Fatalf("expected v2@version2, got %s@version%d", vals[0].Data, vals[0].Context.Version)
	}
}

func TestPutDifferentNodesCreatesSiblings(t *testing.T) {
	s := NewStorage()
	s.Put("k", types.Value{Data: "a", Context: types.Context{Version: 1, NodeID: "n1"}})
	s.Put("k", types.Value{Data: "b", Context: types.Context{Version: 1, NodeID: "n2"}})

	vals, _ := s.Get("k")
	if len(vals) != 2 {
		t.Fatalf("expected 2 siblings, got %d", len(vals))
	}
}
