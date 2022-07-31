package main

import (
	"github.com/arana-db/arana/cmd/admin"
	"github.com/arana-db/arana/testdata"
)

func main() {
	bootstrap := testdata.Path("../conf/bootstrap.yaml")
	_ = admin.Run(bootstrap, ":8080")
}
