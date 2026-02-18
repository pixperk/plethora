package main

import (
	"fmt"
	"log"
	"net"

	"github.com/pixperk/plethora/node"
	pb "github.com/pixperk/plethora/proto"
	"github.com/pixperk/plethora/ring"
	"github.com/pixperk/plethora/server"
	"github.com/pixperk/plethora/types"
	"google.golang.org/grpc"
)

func main() {
	const numNodes = 10
	const Q = 20
	const N = 3
	const R = 2
	const W = 2

	nodes := make([]*node.Node, numNodes)
	for i := range numNodes {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			log.Fatal(err)
		}
		addr := lis.Addr().String()
		n := node.NewNode(fmt.Sprintf("node-%d", i+1), addr)
		nodes[i] = n

		grpcServer := grpc.NewServer()
		pb.RegisterKVServer(grpcServer, server.NewServer(n))
		go grpcServer.Serve(lis)

		fmt.Printf("[BOOT] %s listening on %s\n", n.NodeID, addr)
	}

	r, err := ring.NewRing(Q, N, R, W, nodes)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\n[RING] Q=%d N=%d R=%d W=%d nodes=%d\n\n", Q, N, R, W, numNodes)

	for i, p := range r.Partitions {
		fmt.Printf("[PARTITION] %2d -> %s\n", i, p.Token.NodeID)
	}
	fmt.Println()

	keys := []string{"user:alice", "user:bob", "user:charlie", "order:1001", "order:1002"}
	vals := []string{"Alice Smith", "Bob Jones", "Charlie Brown", "Widget x3", "Gadget x1"}

	for i, key := range keys {
		owner := r.Lookup(key)
		plist := r.PreferenceList(types.Key(key))
		replicaIDs := make([]string, len(plist))
		for j, n := range plist {
			replicaIDs[j] = n.NodeID
		}

		fmt.Printf("[PUT] key=%q val=%q  coordinator=%s  replicas=%v\n", key, vals[i], owner.NodeID, replicaIDs)
		if err := r.Put(types.Key(key), vals[i], nil); err != nil {
			fmt.Printf("[PUT] ERROR: %v\n", err)
		}
	}
	fmt.Println()

	for _, key := range keys {
		result, err := r.Get(types.Key(key))
		if err != nil {
			fmt.Printf("[GET] key=%q ERROR: %v\n", key, err)
			continue
		}
		for _, v := range result {
			fmt.Printf("[GET] key=%q val=%q clock=%v\n", key, v.Data, v.Clock)
		}
	}
	fmt.Println()

	fmt.Println("[UPDATE] read-modify-write on user:alice")
	result, _ := r.Get(types.Key("user:alice"))
	ctx := result[0].Clock
	fmt.Printf("[UPDATE] read clock=%v\n", ctx)

	if err := r.Put(types.Key("user:alice"), "Alice Updated", ctx); err != nil {
		fmt.Printf("[UPDATE] ERROR: %v\n", err)
	}

	result, _ = r.Get(types.Key("user:alice"))
	for _, v := range result {
		fmt.Printf("[UPDATE] key=%q val=%q clock=%v\n", "user:alice", v.Data, v.Clock)
	}
	fmt.Println()

	result, err = r.Get(types.Key("nonexistent"))
	fmt.Printf("[GET] key=%q vals=%v err=%v\n", "nonexistent", result, err)
}
