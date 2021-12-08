unit-test:
	go test ./pkg/... ./third_party/... -coverprofile=coverage.txt -covermode=atomic

build:
	@mkdir -p dist
	cd cmd && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../dist/kylin

docker-build: build
	docker build -f docker/Dockerfile -t kylin:latest .

integration-test: build docker-build
	@mkdir -p docker/data
	@mkdir -p docker/mysqld
	docker-compose -f docker/docker-compose.yaml up -d
	@sleep 30
	@go clean -testcache
	go test -tags integration -v ./test/...
	docker-compose -f docker/docker-compose.yaml down
	@rm -rf dist
	@rm -rf docker/data
	@rm -rf docker/mysqld

clean:
	@rm -rf coverage.txt
	@rm -rf dist
	@rm -rf docker/data
	@rm -rf docker/mysqld