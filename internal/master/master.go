package master

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jalala984/master-worker/api"
	"github.com/jalala984/master-worker/internal/server"
	"google.golang.org/grpc"
)

type Master struct {
	grpcServer *grpc.Server       // The grpc server is for the worker/client connections while the nodeServer handles the logic
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

	// Get the directory path from the query string
	dirPath := r.URL.Query().Get("dir")
	if dirPath == "" {
		http.Error(w, "Missing 'dir' parameter", http.StatusBadRequest)
		return
	}

	// Scan the directory
	files, err := os.ReadDir(dirPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read dir: %v", err), http.StatusInternalServerError)
		return
	}

	count := 0
	for _, file := range files {
		// Only process .sh files
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sh") {
			fullPath := filepath.Join(dirPath, file.Name())

			// Create a unique Task
			task := server.Task{
				ID:     fmt.Sprintf("task-%d-%s", time.Now().UnixNano(), file.Name()),
				Script: fullPath, // Send the path for now
			}

			// Drop it into the channel
			m.nodeServer.TaskQueue <- task
			count++
		}
	}

	fmt.Fprintf(w, "Enqueued %d scripts from %s\n", count, dirPath)
}
