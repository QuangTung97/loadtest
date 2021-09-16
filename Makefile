.PHONY: lint test

lint:
	go fmt ./...
	golint ./...
	go vet ./...

test:
	go test -v ./...