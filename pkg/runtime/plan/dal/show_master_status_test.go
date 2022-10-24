package dal

import (
	"testing"
)

import (
	"github.com/arana-db/parser"

	"github.com/stretchr/testify/assert"
)

func TestSQL(t *testing.T) {
	sql := "SHOW MASTER STATUS"

	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	assert.Nil(t, err)
	assert.NotNil(t, stmtNodes)
}
