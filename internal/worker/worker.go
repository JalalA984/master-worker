package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/jalala984/master-worker/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	client api.NodeServiceClient // This is the gRPC client/worker to communicate with the Master which is defined in api package proto file
	conn   *grpc.ClientConn      // This defines a connection to the gRPC server and is required as defined by gRPC library and protobuf
}

func NewWorker(target string) (*Worker, error) { // target is the address of the Master server
	// "Dial" the Master's gRPC server
	// We use insecure credentials because we haven't set up SSL/TLS certificates yet TODO: add TLS but very later
	// NewClient is the modern replacement for Dial
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return &Worker{
		client: api.NewNodeServiceClient(conn),
		conn:   conn,
	}, nil
}

func (w *Worker) Start() error {
	fmt.Println("Worker connecting to Master...")

	// Setup Signal Catching
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Call AssignTask to open the stream
	// We use an empty Request for now as the "trigger" TODO: later this could instead send worker info like CPU/RAM or other metadata
	stream, err := w.client.AssignTask(context.Background(), &api.Request{Action: "ready"})
	if err != nil {
		return err
	}

	taskChan := make(chan *api.Response)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	// Background Goroutine to pull tasks from the gRPC stream
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			taskChan <- res
		}
	}()

	// Main Loop forever and wait for messages from the Master or shutdown sig
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[SHUTDOWN] Signal received. Waiting for active tasks to finish...")
			wg.Wait() // Ensure the current bash script finishes
			fmt.Println("[SHUTDOWN] Cleanup complete. Exiting.")
			return nil

		case err := <-errChan:
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("stream error: %v", err)

		case res := <-taskChan:
			wg.Add(1)
			// Execute task (wrapped in a func to ensure WaitGroup works)
			func(task *api.Response) {
				defer wg.Done()
				fmt.Printf("EXECUTING TASK %s: %s\n", task.TaskId, task.Data)

				cmd := exec.Command("/bin/bash", task.Data)
				output, err := cmd.CombinedOutput()

				status := "success"
				if err != nil {
					status = fmt.Sprintf("failed: %v", err)
				}

				podName, _ := os.Hostname()
				_, reportErr := w.client.ReportStatus(context.Background(), &api.Request{
					Action:   "completed",
					TaskId:   task.TaskId,
					Payload:  string(output),
					WorkerId: podName,
				})

				if reportErr != nil {
					fmt.Printf("Failed to report task %s: %v\n", task.TaskId, reportErr)
				} else {
					fmt.Printf("Task %s reported as %s\n", task.TaskId, status)
				}
			}(res)
		}
	}
}

func (w *Worker) Close() {
	w.conn.Close()
}
