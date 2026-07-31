.PHONY: default build test lint proto clean

default: build test

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

# Regenerate the Go stubs from proto/*.proto. Requires buf, protoc-gen-go and
# protoc-gen-connect-go on PATH. The generated files are committed, so CI never
# runs codegen.
proto:
	buf generate

clean:
	rm -f proto/*/v1/*.pb.go
	rm -rf proto/*/v1/*connect
