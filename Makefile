.PHONY: help proto build test test-cover lint docker deploy destroy clean loadtest chaos

BINARY   := master-worker
IMAGE    := master-worker:v1
CLUSTER  := mw-cluster

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

proto: ## Regenerate protobuf Go code
	protoc api/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

build: ## Build the binary
	CGO_ENABLED=0 go build -o $(BINARY) .

test: ## Run all tests
	go test ./... -v -race -count=1

test-cover: ## Run tests with coverage report
	go test ./... -v -race -coverprofile=coverage.out -covermode=atomic
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

lint: ## Run golangci-lint
	golangci-lint run ./...

docker: ## Build Docker image
	docker build -t $(IMAGE) .

deploy: ## Deploy to Kind cluster (build + create cluster + helm install)
	./deploy.sh

destroy: ## Tear down Kind cluster
	./destroy.sh

clean: ## Remove build artifacts
	rm -f $(BINARY) coverage.out coverage.html
	go clean ./...

loadtest: build ## Run load test against local master
	go run cmd/loadtest/main.go

chaos: ## Run chaos test (requires running K8s cluster)
	./scripts/chaos_test.sh
