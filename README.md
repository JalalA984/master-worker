The Master starts: It opens a gRPC port (50051) and an HTTP port (9092). It’s now sitting and waiting.

The Worker starts: It connects to 50051 and says "ready". The Master’s AssignTask function starts but "parks" at the CmdChannel.

The Human (You): You send a POST request to 9092.

The Handshake: The HTTP handler drops your message into the CmdChannel. This "wakes up" the gRPC loop, which sends that message across the network to the Worker.


go run main.go master

go run main.go worker

curl -X POST "http://localhost:9092/tasks?cmd=HelloFromHuman"

______________________________________________________________________________________________

Some Ideas:
- A scripts folder? or like a user/human has a bunch of scripts they want to run (that of course do outputs) and instend of sending just string commands the specify maybe path to a script or batch of scripts and then the master assigns jobs/scripts to workers... hmm 

- what if we had 2 masters or similar to how gfs/hadoop have a shadow backup... but then again why do we need backup/more than one master...

other: concensus algo, service discovery? idk?

graceful shutdown ie fault tolerance what if master crashes what if worker crashes mid script. do we need logs ie WALs

- kubernetes/helm impl