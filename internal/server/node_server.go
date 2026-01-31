package server

import (
	"context"
	"fmt"
	"sync"

	api "github.com/jalala984/master-worker/api"
)

// Task represents a single script to be executed
type Task struct {
	ID     string
	Script string
}

// NodeServer implements the gRPC NodeService defined in the proto file.
type NodeServer struct {
	// This embed is required by gRPC for forward compatibility
	api.UnimplementedNodeServiceServer

	// TaskQueue is used to push tasks from the Master's API to the connected Workers
	TaskQueue chan Task

	// Tracking "In-Flight" tasks
	mu          sync.RWMutex
	ActiveTasks map[string]Task
}

// NewNodeServer is a constructor that initializes our server safely.
func NewNodeServer() *NodeServer {
	return &NodeServer{
		TaskQueue:   make(chan Task, 100),
		ActiveTasks: make(map[string]Task),
	}
}

func (s *NodeServer) ReportStatus(ctx context.Context, req *api.Request) (*api.Response, error) {
	if req.Action == "completed" {
		s.mu.Lock()
		// Remove from memory so we stop tracking it
		delete(s.ActiveTasks, req.TaskId)
		s.mu.Unlock()

		fmt.Printf("Task %s finalized. Output: %s\n", req.TaskId, req.Payload)
	}

	return &api.Response{Data: "acknowledged"}, nil
}

// AssignTask handles a Server Streaming RPC (1 request -> many responses).
// Basically the loop says "whenever there's a new command in the channel, send it to the worker (the worker is the client)" otherwise if there is a context done signal, exit the loop
func (s *NodeServer) AssignTask(req *api.Request, stream api.NodeService_AssignTaskServer) error {
	// Keep track of which tasks this specific worker instance has taken so we can recover them if the worker dies.
	localInFlight := make(map[string]Task)

	for {
		select {
		case <-stream.Context().Done():
			s.mu.Lock()
			for id, task := range localInFlight {
				// Check if the task is STILL in the global active map
				// If it's NOT there, it means the worker actually finished it
				// and ReportStatus already deleted it.
				if _, exists := s.ActiveTasks[id]; exists {
					fmt.Printf("Worker disconnected. Recovering unfinished task: %s\n", id)
					delete(s.ActiveTasks, id)
					s.TaskQueue <- task
				}
			}
			s.mu.Unlock()
			return stream.Context().Err()

		case task := <-s.TaskQueue:
			// Register the task globally and locally
			s.mu.Lock()
			s.ActiveTasks[task.ID] = task
			localInFlight[task.ID] = task
			s.mu.Unlock()

			fmt.Printf("Dispensing task %s to worker\n", task.ID)

			// Send to worker
			resp := &api.Response{
				Data:   task.Script,
				TaskId: task.ID,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}
