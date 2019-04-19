all: build

build:
	go build -a -v -o output/oss

fmt:
	find ./ -name "*.go" | grep -v "/vendor/" | xargs gofmt -w

clean:
	rm -rf output
