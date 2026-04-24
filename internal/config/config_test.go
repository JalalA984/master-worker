package config

import (
	"os"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	cfg := Load()
	if cfg.GRPCPort != ":50051" {
		t.Fatalf("expected :50051, got %s", cfg.GRPCPort)
	}
	if cfg.HTTPPort != ":9092" {
		t.Fatalf("expected :9092, got %s", cfg.HTTPPort)
	}
	if cfg.MasterAddr != "localhost:50051" {
		t.Fatalf("expected localhost:50051, got %s", cfg.MasterAddr)
	}
	if cfg.HeartbeatInterval != 10*time.Second {
		t.Fatalf("expected 10s, got %v", cfg.HeartbeatInterval)
	}
}

func TestEnvOverride(t *testing.T) {
	os.Setenv("GRPC_PORT", ":8888")
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Unsetenv("GRPC_PORT")
	defer os.Unsetenv("LOG_LEVEL")

	cfg := Load()
	if cfg.GRPCPort != ":8888" {
		t.Fatalf("expected :8888, got %s", cfg.GRPCPort)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("expected debug, got %s", cfg.LogLevel)
	}
}

func TestDurationParsing(t *testing.T) {
	os.Setenv("HEARTBEAT_INTERVAL", "5s")
	defer os.Unsetenv("HEARTBEAT_INTERVAL")

	cfg := Load()
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Fatalf("expected 5s, got %v", cfg.HeartbeatInterval)
	}
}
