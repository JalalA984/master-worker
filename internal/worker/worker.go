package worker

import (
	"context"
	"fmt"
	"io"

	"github.com/jalala984/master-worker/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	client api.NodeServiceClient // This is the gRPC client/worker to communicate with the Master which is defined in api package proto file
	conn   *grpc.ClientConn // This defines a connection to the gRPC server and is required as defined by gRPC library and protobuf
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
			// The Master closed the stream gracefully
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving from stream: %v", err)
		}

		// Logic: What to do with the command?
		fmt.Printf("RECEIVED COMMAND: %s\n", res.Data)
		
		// For now, we just print it. Later we can make it execute shell commands.
	}

	return nil
}

func (w *Worker) Close() {
	w.conn.Close()
}