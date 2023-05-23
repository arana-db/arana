package runtime

import (
	"context"
	"fmt"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBackendResourcePool_Get(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		bcp     BackendResourcePool
		args    args
		want    *mysql.BackendConnection
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.bcp.Get(tt.args.ctx)
			if !tt.wantErr(t, err, fmt.Sprintf("Get(%v)", tt.args.ctx)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Get(%v)", tt.args.ctx)
		})
	}
}
