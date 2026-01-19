package master

import (
	"fmt"
	"net"
	"net/http"

	"github.com/jalala984/master-worker/api"
	"github.com/jalala984/master-worker/internal/server"
	"google.golang.org/grpc"
)

type Master struct {
	grpcServer *grpc.Server // The grpc server is for the worker/client connections while the nodeServer handles the logic
	nodeServer *server.NodeServer // This is the implementation internal/server/nodeserver.go
}

func NewMaster() *Master { // A master node is defined by having a gRPC server and a node server. We have two separate servers because the gRPC server is generic while the node server has our specific logic.
	return &Master{
		grpcServer: grpc.NewServer(),
		nodeServer: server.NewNodeServer(),
	}
}

func (m *Master) Start() error {
	// A master node starts the gRPC listener (port 50051)
	// Can we have more than one master? Yes, but they need to coordinate between themselves TODO: later after MVP
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return err
	}

	// Register our NodeServer logic with the gRPC system -- this is defined in the proto file
	api.RegisterNodeServiceServer(m.grpcServer, m.nodeServer)

	// Start gRPC in the background (goroutine)
	fmt.Println("gRPC Master Server listening on :50051")
	go m.grpcServer.Serve(lis)
	// Shouldn't we defer stopping the gRPC server? Yes, but we never reach that point in this simple example. TODO: later because we need graceful shutdown

	// Start the HTTP API (port 9092) -- this is for us humans to interact with the master and send tasks as HTTP requests -- wait so what are the clients again? The workers! The workers connect to the master via gRPC but we humans connect to the master via HTTP to send tasks
	// This endpoint allows us to send tasks to the workers
	http.HandleFunc("/tasks", m.handleTask)

	fmt.Println("HTTP API listening on :9092")
	return http.ListenAndServe(":9092", nil)
}

func (m *Master) handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the command from the URL query or body
	cmd := r.URL.Query().Get("cmd")
	if cmd == "" {
		http.Error(w, "Missing cmd parameter", http.StatusBadRequest)
		return
	}

	// We send the command into the channel. 
	// The gRPC stream is waiting on the other side of this channel!
	m.nodeServer.CmdChannel <- cmd // this is blocking if no workers are connected, which is fine for this simple example but maybe later TODO: add buffering or worker management?

	fmt.Fprintf(w, "Task '%s' sent to workers\n", cmd)
}