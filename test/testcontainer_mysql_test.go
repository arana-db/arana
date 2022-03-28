package test_test

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
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
	rows, err := db.Query(`SELECT emp_no, birth_date, first_name, last_name, gender, hire_date FROM employees 
		WHERE emp_no = ?`, 100001)
	assert.NoErrorf(t, err, "select row error: %v", err)

	var empNo string
	var birthDate time.Time
	var firstName string
	var lastName string
	var gender string
	var hireDate time.Time
	if rows.Next() {
		err = rows.Scan(&empNo, &birthDate, &firstName, &lastName, &gender, &hireDate)
		if err != nil {
			t.Error(err)
		}
	}
	assert.Equal(t, "scott", firstName)
}
