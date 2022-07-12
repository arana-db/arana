package dataset

import (
	"fmt"
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestVirtualDataset_Close(t *testing.T) {
	type fields struct {
		Columns []proto.Field
		Rows    []proto.Row
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{"TestVirtualDataset_Close_1", fields{}, nil},
		{"TestVirtualDataset_Close_2", fields{}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cu := &VirtualDataset{
				Columns: tt.fields.Columns,
				Rows:    tt.fields.Rows,
			}
			assert.NoError(t, cu.Close())
		})
	}
}

func TestVirtualDataset_Fields(t *testing.T) {
	type fields struct {
		Columns []proto.Field
		Rows    []proto.Row
	}
	tests := []struct {
		name    string
		fields  fields
		want    []proto.Field
		wantErr assert.ErrorAssertionFunc
	}{
		{"TestVirtualDataset_Fields", fields{createFields(), nil}, createFields(), createErrorAssertionFunc()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cu := &VirtualDataset{
				Columns: tt.fields.Columns,
				Rows:    tt.fields.Rows,
			}
			got, err := cu.Fields()
			if !tt.wantErr(t, err, fmt.Sprintf("Fields()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "Fields()")
		})
	}
}

func TestVirtualDataset_Next(t *testing.T) {
	t.Run("TestVirtualDataset_Next", func(t *testing.T) {
		cu := &VirtualDataset{
			Columns: createFields(),
			Rows:    createRows(),
		}
		i := 0
		for {
			got, err := cu.Next()
			if err == io.EOF {
				break
			}
			assert.Equalf(t, rows.NewTextVirtualRow(createFields(), []proto.Value{
				int64(i),
				fmt.Sprintf("fake-name-%d", i),
				int64(i),
			}), got, "Next()")
			i++
		}
	})
}

func createFields() []proto.Field {
	return []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
		mysql.NewField("gender", consts.FieldTypeLong),
	}
}

func createRows() []proto.Row {
	var result []proto.Row
	for i := 0; i < 10; i++ {
		result = append(result, rows.NewTextVirtualRow(createFields(), []proto.Value{
			int64(i),
			fmt.Sprintf("fake-name-%d", i),
			int64(i),
		}))
	}
	return result
}

func createErrorAssertionFunc() assert.ErrorAssertionFunc {
	return func(assert.TestingT, error, ...interface{}) bool {
		return true
	}
}
