import sys, os, platform
print(f"Python {sys.version}")
print(f"Host: {platform.node()}")
print(f"PID: {os.getpid()}")
print("Hello from Python worker!")
