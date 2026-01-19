package server

import (
	"context"
	"fmt"

	api "github.com/jalala984/master-worker/api"
)

// NodeServer implements the gRPC NodeService defined in the proto file.
type NodeServer struct {
	// This embed is required by gRPC for forward compatibility
	api.UnimplementedNodeServiceServer
	
	// CmdChannel is used to push tasks from the Master's API to the connected Workers
	CmdChannel chan string
}

// NewNodeServer is a constructor that initializes our server safely.
func NewNodeServer() *NodeServer {
	return &NodeServer{
		CmdChannel: make(chan string),
	}
}

// ReportStatus handles a simple Unary RPC (1 request -> 1 response).
func (s *NodeServer) ReportStatus(ctx context.Context, req *api.Request) (*api.Response, error) {
	return &api.Response{Data: "ok"}, nil
}

// AssignTask handles a Server Streaming RPC (1 request -> many responses).
// Basically the loop says "whenever there's a new command in the channel, send it to the worker (the worker is the client)" otherwise if there is a context done signal, exit the loop
func (s *NodeServer) AssignTask(req *api.Request, stream api.NodeService_AssignTaskServer) error {
	fmt.Printf("Worker joined with action: %s\n", req.Action)
	stream.Send(&api.Response{Data: "Connection established. Waiting for tasks..."})
	
	for {
		select {
		// Listen for the context being cancelled (e.g., worker disconnects)
		case <-stream.Context().Done():
			return stream.Context().Err()
		
		// Wait for a command to be sent into the channel
		case cmd := <-s.CmdChannel:
			if err := stream.Send(&api.Response{Data: cmd}); err != nil {
				return err
			}
		}
	}
}