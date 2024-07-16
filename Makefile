# Makefile

# Build the Go CLI tool
build:
	go build -o tsb cmd/tsb/main.go
	chmod +x tsb
