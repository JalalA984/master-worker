package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jalala984/master-worker/internal/master"
	"github.com/jalala984/master-worker/internal/worker"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go [master|worker]")
		return
	}

	role := os.Args[1]

	switch role {
	case "master":
		fmt.Println("--- Master Node Mode ---")
		m := master.NewMaster()
		// master opens a gRPC server on port 50051 and a HTTP server on port 9092
		// Start() blocks because of the HTTP server.
		if err := m.Start(); err != nil {
			log.Fatalf("Master crashed: %v", err)
		}

	case "worker":
		fmt.Println("--- Worker Node Mode ---")
		// The worker "dials" the Master on its gRPC port
		w, err := worker.NewWorker("localhost:50051")
		
		if err != nil {
			log.Fatalf("Worker failed to connect: %v", err)
		}
		defer w.Close()

		// Start() blocks because it's listening to the gRPC stream.
		// worker starts and connects to master gRPC server and the masters AssignTask has started but is blocked waiting for tasks inside CmdChannel
		if err := w.Start(); err != nil { 
			log.Fatalf("Worker lost connection: %v", err)
		}

	default:
		fmt.Printf("Unknown role: %s. Use 'master' or 'worker'.\n", role)
	}
}
