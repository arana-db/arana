VERSION=$(shell cat "./VERSION" 2> /dev/null)

GO_FLAGS := -ldflags "-X main.Branch=$(GIT_BRANCH) -X main.Revision=$(GIT_REVISION) -X main.Version=$(VERSION) -extldflags \"-static\" -s -w" -tags netgo
GO = go
GO_PATH = $(shell $(GO) env GOPATH)
GO_OS = $(shell $(GO) env GOOS)
ifeq ($(GO_OS), darwin)
    GO_OS = mac
endif

# License environment
GO_LICENSE_CHECKER_DIR = license-header-checker-$(GO_OS)
GO_LICENSE_CHECKER = $(GO_PATH)/bin/license-header-checker
LICENSE_DIR = /tmp/tools/license

unit-test:
	go test ./pkg/... ./third_party/... -coverprofile=coverage.txt -covermode=atomic

# Generate binaries for a Cortex release
dist dist/arana-linux-amd64 dist/arana-darwin-amd64 dist/arana-linux-amd64-sha-256 dist/arana-darwin-amd64-sha-256:
	rm -fr ./dist
	mkdir -p ./dist
	GOOS="linux"  GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana-linux-amd64 ./cmd
	GOOS="darwin" GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana-darwin-amd64 ./cmd
	sha256sum ./dist/arana-darwin-amd64 | cut -d ' ' -f 1 > ./dist/arana-darwin-amd64-sha-256
	sha256sum ./dist/arana-linux-amd64  | cut -d ' ' -f 1 > ./dist/arana-linux-amd64-sha-256

# Generate binaries for a Cortex release
build dist/arana dist/arana-sha-256:
	rm -fr ./dist
	mkdir -p ./dist
	CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/arana ./cmd
	sha256sum ./dist/arana  | cut -d ' ' -f 1 > ./dist/arana-sha-256

docker-build:
	docker build -t aranadb/arana:latest .

integration-test:
	@go clean -testcache
	go test -tags integration -v ./test/...

clean:
	@rm -rf coverage.txt
	@rm -rf dist

prepareLic:
	echo 'The makefile is for ci test and has dependencies. Do not run it locally. If you want to run the unit tests, run command `go test ./...` directly.'
	$(GO_LICENSE_CHECKER) -version || (wget https://github.com/lsm-dev/license-header-checker/releases/download/v1.2.0/$(GO_LICENSE_CHECKER_DIR).zip -O $(GO_LICENSE_CHECKER_DIR).zip && unzip -o $(GO_LICENSE_CHECKER_DIR).zip && mkdir -p $(GO_PATH)/bin/ && cp $(GO_LICENSE_CHECKER_DIR)/64bit/license-header-checker $(GO_PATH)/bin/)
	ls /tmp/tools/license/license.txt || wget -P $(LICENSE_DIR) https://github.com/dubbogo/resources/raw/master/tools/license/license.txt

.PHONY: license
license: prepareLic
	$(GO_LICENSE_CHECKER) -v -a -r -i vendor $(LICENSE_DIR)/license.txt . go && [[ -z `git status -s` ]]