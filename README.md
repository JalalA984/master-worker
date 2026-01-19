TODO: either 2 or more masters and thus some kind of service discovery and concensus OR some kind of replication methodology for backup/shadow master
TODO: look at other todos in internal/master

The Master starts: It opens a gRPC port (50051) and an HTTP port (9092). It’s now sitting and waiting.

The Worker starts: It connects to 50051 and says "ready". The Master’s AssignTask function starts but "parks" at the CmdChannel.

The Human (You): You send a POST request to 9092.

The Handshake: The HTTP handler drops your message into the CmdChannel. This "wakes up" the gRPC loop, which sends that message across the network to the Worker.


go run main.go master

go run main.go worker

curl -X POST "http://localhost:9092/tasks?cmd=HelloFromHuman"