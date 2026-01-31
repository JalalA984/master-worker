package worker

import (
	"context"
	"fmt"
	"io"
	"os/exec"

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

	// Call AssignTask to open the stream
	// We use an empty Request for now as the "trigger" TODO: later this could instead send worker info like CPU/RAM or other metadata
	stream, err := w.client.AssignTask(context.Background(), &api.Request{Action: "ready"})
	if err != nil {
		return err
	}

	// Loop forever and wait for messages from the Master
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fmt.Printf("EXECUTING TASK %s: %s\n", res.TaskId, res.Data)

		// Run the script
		// In a prod app, save the bytes to a file; here assume the path is accessible
		cmd := exec.Command("/bin/bash", res.Data)
		output, err := cmd.CombinedOutput()

		status := "success"
		if err != nil {
			status = fmt.Sprintf("failed: %v", err)
		}

		// Report completion back to Master
		_, err = w.client.ReportStatus(context.Background(), &api.Request{
			Action:  "completed",
			TaskId:  res.TaskId,
			Payload: string(output), // Send the script output back!
		})
		if err != nil {
			fmt.Printf("Failed to report task %s: %v\n", res.TaskId, err)
		} else {
			fmt.Printf("Task %s reported as %s\n", res.TaskId, status)
		}
	}
	return nil
}

func (w *Worker) Close() {
	w.conn.Close()
}
