VERSION=$(shell cat "./VERSION" 2> /dev/null)

GO_FLAGS := -ldflags "-X main.Branch=$(GIT_BRANCH) -X main.Revision=$(GIT_REVISION) -X main.Version=$(VERSION) -extldflags \"-static\" -s -w" -tags netgo

unit-test:
	go test ./pkg/... ./third_party/... -coverprofile=coverage.txt -covermode=atomic

# Generate binaries for a Cortex release
dist dist/arana-linux-amd64 dist/arana-darwin-amd64 dist/arana-linux-amd64-sha-256 dist/arana-darwin-amd64-sha-256:
	rm -fr ./dist
	mkdir -p ./dist
	GOOS="linux"  GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana-linux-amd64   ./cmd
	GOOS="darwin" GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana-darwin-amd64  ./cmd
	sha256sum ./dist/arana-darwin-amd64 | cut -d ' ' -f 1 > ./dist/arana-darwin-amd64-sha-256
	sha256sum ./dist/arana-linux-amd64  | cut -d ' ' -f 1 > ./dist/arana-linux-amd64-sha-256

# Generate binaries for a Cortex release
build dist/arana dist/arana-sha-256:
	rm -fr ./dist
	mkdir -p ./dist
	CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana   ./cmd
	sha256sum ./dist/arana  | cut -d ' ' -f 1 > ./dist/arana-sha-256

docker-build: build
	docker build -f docker/Dockerfile -t arana:latest .

integration-test: build docker-build
	@mkdir -p docker/data
	@mkdir -p docker/mysqld
	docker-compose -f docker/docker-compose.yaml up -d
	@sleep 30
	@go clean -testcache
	go test -tags integration -v ./test/...

clean:
	docker-compose -f docker/docker-compose.yaml down
	@rm -rf coverage.txt
	@rm -rf dist
	@rm -rf docker/data
	@rm -rf docker/mysqld