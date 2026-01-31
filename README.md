The Master starts: It opens a gRPC port (50051) and an HTTP port (9092). It’s now sitting and waiting.

The Worker starts: It connects to 50051 and says "ready". The Master’s AssignTask function starts but "parks" at the task channel.

The Human (You): You send a POST request to 9092.

The Handshake: The HTTP handler drops your message into the task channel. This "wakes up" the gRPC loop, which sends that message across the network to the Worker.


go run main.go master

go run main.go worker

curl -X POST "http://localhost:9092/tasks?dir=$(pwd)/test_scripts"

# Build the image
docker build -t master-worker:v1 .

# Create a network
docker network create my-net

# Start the Master
# map 9092 (HTTP) and 50051 (gRPC) to our host
# Run master with a volume mount
docker run -d --name master-node --network my-net \
  -v $(pwd)/test_scripts:/scripts \
  -p 9092:9092 -p 50051:50051 \
  master-worker:v1 ./main master

# Start the Worker
# Note: Target is "master-node:50051" not localhost
docker run -d --name worker-node --network my-net \
  -e MASTER_ADDR=master-node:50051 \
  -v $(pwd)/test_scripts:/scripts \
  master-worker:v1 ./main worker


# Trigger it
curl -X POST "http://localhost:9092/tasks?dir=/scripts"

docker stop worker-node && docker rm worker-node
docker stop master-node && docker rm master-node

______________________________________________________________________________________________

Some Ideas:
- A scripts folder? or like a user/human has a bunch of scripts they want to run (that of course do outputs) and instend of sending just string commands the specify maybe path to a script or batch of scripts (right now lets just support bash scripts that print 1-n where the n files are the bash script files) and then the master assigns jobs/scripts to workers... hmm 

- what if we had 2 masters or similar to how gfs/hadoop have a shadow backup... but then again why do we need backup/more than one master...

other: concensus algo, service discovery? idk?

graceful shutdown ie fault tolerance what if master crashes what if worker crashes mid script. do we need logs ie WALs

- kubernetes/helm impl