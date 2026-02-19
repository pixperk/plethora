package server

import (
	"context"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/pixperk/plethora/client"
	"github.com/pixperk/plethora/merkle"
	"github.com/pixperk/plethora/node"
	pb "github.com/pixperk/plethora/proto"
	"github.com/pixperk/plethora/types"
	"github.com/pixperk/plethora/vclock"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedKVServer
	node *node.Node

	//hinted handoff
	peers    map[string]string //nodeID -> addr of peer nodes for hint forwarding
	addrToID map[string]string //addr -> nodeID for reverse lookup when receiving hints
	alive    map[string]bool   //nodeID -> alive status of peer nodes

	aliveMu sync.RWMutex

	// anti-entropy: nodes that share key ranges with this node
	replicaPeers []*node.Node
}

func NewServer(n *node.Node, allNodes []*node.Node) *Server {
	peers := make(map[string]string)
	addrToID := make(map[string]string)
	alive := make(map[string]bool)
	for _, node := range allNodes {
		if node.NodeID == n.NodeID {
			continue
		}
		peers[node.NodeID] = node.Addr
		addrToID[node.Addr] = node.NodeID
		alive[node.NodeID] = true // assume all nodes start alive; in a real system we'd want a more robust health check
	}

	return &Server{
		node:     n,
		peers:    peers,
		addrToID: addrToID,
		alive:    alive,
	}
}

// SetReplicaPeers sets the nodes this server should sync with during anti-entropy.
func (s *Server) SetReplicaPeers(peers []*node.Node) {
	s.replicaPeers = peers
}

func (s *Server) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	val := fromProtoValue(req.Value)
	s.node.Store(types.Key(req.Key), val)
	return &pb.PutResponse{}, nil
}

func (s *Server) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	vals, found := s.node.Get(types.Key(req.Key))
	return &pb.GetResponse{
		Values: toProtoValues(vals),
		Found:  found,
	}, nil
}

// StoreHint stores a hinted value for a target node that is currently unreachable.
// The coordinator calls this when it fails to forward a write to a replica node, and the target node will attempt to apply the hinted writes when it recovers.
func (s *Server) HintedPut(_ context.Context, req *pb.HintedPutRequest) (*pb.PutResponse, error) {
	val := fromProtoValue(req.Value)
	s.node.StoreHint(req.TargetNodeId, types.Key(req.Key), val)
	return &pb.PutResponse{}, nil
}

// Start listens on the node's address and serves gRPC requests. Blocks until the server stops.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.node.Addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, s)

	go s.runHandoff()
	go s.runAntiEntropy()

	return grpcServer.Serve(lis)
}

func (s *Server) Heartbeat(stream pb.KV_HeartbeatServer) error {
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	peerID := msg.NodeId

	s.aliveMu.Lock()
	s.alive[peerID] = true
	s.aliveMu.Unlock()

	//keep receiving heartbeats until the stream is closed,
	// if we get an error receiving, mark the peer as dead and exit
	for {
		_, err := stream.Recv()
		if err != nil {
			// stream broke = peer is down
			s.aliveMu.Lock()
			s.alive[peerID] = false
			s.aliveMu.Unlock()
			return err
		}
	}

}

// GetKeyHashes returns the key-hash pairs from this node's storage.
// The caller builds merkle trees locally and diffs them to find divergent keys.
func (s *Server) GetKeyHashes(_ context.Context, _ *pb.GetKeyHashesRequest) (*pb.GetKeyHashesResponse, error) {
	kh := s.node.Storage.KeyHashes()
	entries := make([]*pb.KeyHashEntry, len(kh))
	for i, e := range kh {
		entries[i] = &pb.KeyHashEntry{Key: e.Key, Hash: e.Hash[:]}
	}
	return &pb.GetKeyHashesResponse{Entries: entries}, nil
}

// SyncKeys returns the actual values for the requested keys.
// Called after a merkle diff identifies which keys diverged.
func (s *Server) SyncKeys(_ context.Context, req *pb.SyncKeysRequest) (*pb.SyncKeysResponse, error) {
	data := make(map[string]*pb.GetResponse, len(req.Keys))
	for _, key := range req.Keys {
		vals, found := s.node.Get(types.Key(key))
		data[key] = &pb.GetResponse{
			Values: toProtoValues(vals),
			Found:  found,
		}
	}
	return &pb.SyncKeysResponse{Data: data}, nil
}

// runHandoff periodically checks for any hints that need to be forwarded to their target nodes and attempts to send them.
func (s *Server) runHandoff() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.aliveMu.RLock()
		for nodeID, up := range s.alive {
			if !up {
				continue
			}
			addr := s.peers[nodeID]
			items := s.node.DrainHints(nodeID)
			for _, item := range items {
				client.RemotePut(addr, item.Key, item.Value)
			}
		}
		s.aliveMu.RUnlock()
	}
}

// runAntiEntropy periodically picks a replica peer, compares merkle trees,
// and syncs any divergent keys.
func (s *Server) runAntiEntropy() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	peerIdx := 0
	for range ticker.C {
		if len(s.replicaPeers) == 0 {
			continue
		}

		// round-robin through replica peers
		peer := s.replicaPeers[peerIdx%len(s.replicaPeers)]
		peerIdx++

		// check if peer is alive
		s.aliveMu.RLock()
		up := s.alive[peer.NodeID]
		s.aliveMu.RUnlock()
		if !up {
			continue
		}

		// get peer's key hashes
		entries, err := client.RemoteGetKeyHashes(peer.Addr)
		if err != nil {
			continue
		}

		// convert proto entries to merkle.KeyHash
		peerHashes := make([]merkle.KeyHash, len(entries))
		for i, e := range entries {
			var h [16]byte
			copy(h[:], e.Hash)
			peerHashes[i] = merkle.KeyHash{Key: e.Key, Hash: h}
		}

		// build both trees locally and diff
		localTree := s.node.MerkleTree()
		peerTree := merkle.Build(peerHashes)
		diffKeys := merkle.Diff(localTree, peerTree)
		if len(diffKeys) == 0 {
			continue
		}

		// fetch divergent values from peer and write to local storage
		data, err := client.RemoteSyncKeys(peer.Addr, diffKeys)
		if err != nil {
			continue
		}
		for key, vals := range data {
			for _, val := range vals {
				s.node.Store(types.Key(key), val)
			}
		}
	}
}

// --- proto <-> types conversion helpers ---

func toProtoValue(v types.Value) *pb.Value {
	return &pb.Value{
		Data:  v.Data,
		Clock: &pb.VectorClock{Entries: v.Clock},
	}
}

func toProtoValues(vals []types.Value) []*pb.Value {
	out := make([]*pb.Value, len(vals))
	for i, v := range vals {
		out[i] = toProtoValue(v)
	}
	return out
}

func fromProtoValue(v *pb.Value) types.Value {
	clock := vclock.NewVClock()
	if v.Clock != nil {
		maps.Copy(clock, v.Clock.Entries)
	}
	return types.Value{Data: v.Data, Clock: clock}
}
