package server

import (
	"context"
	"net"

	"github.com/pixperk/plethora/node"
	pb "github.com/pixperk/plethora/proto"
	"github.com/pixperk/plethora/types"
	"github.com/pixperk/plethora/vclock"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedKVServer
	node *node.Node
}

func NewServer(n *node.Node) *Server {
	return &Server{node: n}
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

// Start listens on the node's address and serves gRPC requests. Blocks until the server stops.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.node.Addr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, s)
	return grpcServer.Serve(lis)
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
		for k, val := range v.Clock.Entries {
			clock[k] = val
		}
	}
	return types.Value{Data: v.Data, Clock: clock}
}
