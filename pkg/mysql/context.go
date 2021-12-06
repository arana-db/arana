package mysql

import (
	"context"
)

// Context
type Context struct {
	context.Context

	Conn *Conn

	CommandType byte
	// sql Data
	Data []byte
}
