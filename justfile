set dotenv-load := true

alias r := run
alias c := cli

default:
    @just --list

run:
    @go run ./example/local_server

cli:
    @mycli -h127.0.0.1 -P13306 -udksl employees -p123456
cli-raw:
    @mycli -h127.0.0.1 -uroot employees -p123456

fix:
    @imports-formatter .
    @license-eye header fix


sysbench MODE="run":
  sysbench oltp_read_write  --mysql-user=root --mysql-password=123456 --mysql-host=127.0.0.1 --mysql-port=3306 --mysql-db=employees_0000 --histogram=on --report-interval=1 --time=300  --db-ps-mode=disable  --threads=64 --tables=250 --table_size=25000  --report-interval=1 --percentile=95 --skip-trx=on --mysql-ignore-errors=1062  --forced-shutdown=1 {{MODE}}


sysbench2 MODE="run":
  sysbench oltp_read_write  --mysql-user=arana --mysql-password=123456 --mysql-host=127.0.0.1 --mysql-port=13306 --mysql-db=employees --histogram=on --report-interval=1 --time=300  --db-ps-mode=disable  --threads=8 --tables=250 --table_size=25000  --report-interval=1 --percentile=95 --skip-trx=on --mysql-ignore-errors=1062  --forced-shutdown=1 {{MODE}}
