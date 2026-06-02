.PHONY: build run test vet lint tidy clean

build:
	go build -o bin/scraper ./cmd/scraper

run:
	go run ./cmd/scraper

test:
	go test -race ./...

vet:
	go vet ./...

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin
