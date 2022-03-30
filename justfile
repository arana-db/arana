set dotenv-load := true

alias r := run
alias c := cli

default:
    @just --list

run:
    @go run ./cmd/... start -c ./docker/conf/config.yaml

cli:
    @mycli -h127.0.0.1 -P13306 -udksl employees -p123456
cli-raw:
    @mycli -h127.0.0.1 -uroot employees -p123456
