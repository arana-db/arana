package test_test

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

import (
	_ "github.com/go-sql-driver/mysql"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/test"
)

var (
	db *sql.DB
)

func TestMain(m *testing.M) {
	var err error
	var terminateContainer func() // variable to store function to terminate container
	terminateContainer, db, err = test.SetupMySQLContainer()
	defer terminateContainer() // make sure container will be terminated at the end
	if err != nil {
		log.Error("failed to setup MySQL container")
		panic(err)
	}
	os.Exit(m.Run())
}

func TestSelect_Integration(t *testing.T) {
	rows, err := db.Query(`SELECT uid, name, score, nickname FROM student_0001 where uid = ?`, 1)
	assert.NoErrorf(t, err, "select row error: %v", err)

	var uid uint64
	var name string
	var score float64
	var nickname string

	if rows.Next() {
		err = rows.Scan(&uid, &name, &score, &nickname)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", name)
	assert.Equal(t, "nc_scott", nickname)
}
