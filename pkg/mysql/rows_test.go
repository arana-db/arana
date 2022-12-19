/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mysql

import (
	"bytes"
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/proto"
)

func TestBinaryRow_Fields(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []proto.Field
	}{
		{"Fields_1", fields{[]proto.Field{
			NewField("id", consts.FieldTypeLong),
		}, []byte{}}, []proto.Field{
			NewField("id", consts.FieldTypeLong),
		}},
		{"Fields_2", fields{[]proto.Field{
			NewField("name", consts.FieldTypeVarChar),
			NewField("gender", consts.FieldTypeLong),
		}, []byte{}}, []proto.Field{
			NewField("name", consts.FieldTypeVarChar),
			NewField("gender", consts.FieldTypeLong),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := BinaryRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, bi.Fields(), "Fields()")
		})
	}
}

func TestBinaryRow_Get(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    proto.Value
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"Get",
			fields{createTestFields(), createTestData()},
			args{"name"},
			proto.NewValueString("scott"),
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := NewBinaryRow(
				tt.fields.fields,
				tt.fields.raw,
			)
			got, err := bi.Get(tt.args.name)
			if !tt.wantErr(t, err, fmt.Sprintf("Get(%v)", tt.args.name)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Get(%v)", tt.args.name)
		})
	}
}

func TestBinaryRow_IsBinary(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"IsBinary", fields{createTestFields(), createTestData()}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := BinaryRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, bi.IsBinary(), "IsBinary()")
		})
	}
}

func TestBinaryRow_Length(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"Length", fields{createTestFields(), createTestData()}, 24},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := BinaryRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, bi.Length(), "Length()")
		})
	}
}

func TestBinaryRow_Scan(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	type args struct {
		dest []proto.Value
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{"Scan_0", fields{nil, []byte{0x01}}, args{make([]proto.Value, 3)}, assert.Error},
		{"Scan_1", fields{createTestFields(), createTestData()}, args{make([]proto.Value, 3)}, assert.NoError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := BinaryRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			tt.wantErr(t, bi.Scan(tt.args.dest), fmt.Sprintf("Scan(%v)", tt.args.dest))
		})
	}
}

func TestBinaryRow_WriteTo(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   string
		wantN   int64
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"WriteTo",
			fields{createTestFields(), createTestData()},
			string([]byte{0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x73, 0x63, 0x6f, 0x74, 0x74, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
			24,
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bi := BinaryRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			w := &bytes.Buffer{}
			gotN, err := bi.WriteTo(w)
			if !tt.wantErr(t, err, fmt.Sprintf("WriteTo(%v)", w)) {
				return
			}
			assert.Equalf(t, tt.wantW, w.String(), "WriteTo(%v)", w)
			assert.Equalf(t, tt.wantN, gotN, "WriteTo(%v)", w)
		})
	}
}

func TestNewBinaryRow(t *testing.T) {
	var (
		fields = createTestFields()
		raw    = createTestData()
	)
	type args struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name string
		args args
		want BinaryRow
	}{
		{"NewBinaryRow", args{fields, raw}, BinaryRow{fields, raw}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewBinaryRow(tt.args.fields, tt.args.raw), "NewBinaryRow(%v, %v)", tt.args.fields, tt.args.raw)
		})
	}
}

func TestNewTextRow(t *testing.T) {
	var (
		fields = createTestFields()
		raw    = createTestData()
	)
	type args struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name string
		args args
		want TextRow
	}{
		{"NewBinaryRow", args{fields, raw}, TextRow{fields, raw}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewTextRow(tt.args.fields, tt.args.raw), "NewTextRow(%v, %v)", tt.args.fields, tt.args.raw)
		})
	}
}

func TestTextRow_Fields(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   []proto.Field
	}{
		{"Fields_1", fields{[]proto.Field{
			NewField("id", consts.FieldTypeLong),
		}, []byte{}}, []proto.Field{
			NewField("id", consts.FieldTypeLong),
		}},
		{"Fields_2", fields{[]proto.Field{
			NewField("name", consts.FieldTypeVarChar),
			NewField("gender", consts.FieldTypeLong),
		}, []byte{}}, []proto.Field{
			NewField("name", consts.FieldTypeVarChar),
			NewField("gender", consts.FieldTypeLong),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, te.Fields(), "Fields()")
		})
	}
}

func TestTextRow_Get(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    proto.Value
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"Get",
			fields{createTestFields(), []byte{0x01, 0x31, 0x05, 0x73, 0x63, 0x6f, 0x74, 0x74, 0x01, 0x31}},
			args{"name"},
			proto.NewValueString("scott"),
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			got, err := te.Get(tt.args.name)
			if !tt.wantErr(t, err, fmt.Sprintf("Get(%v)", tt.args.name)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Get(%v)", tt.args.name)
		})
	}
}

func TestTextRow_IsBinary(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"IsBinary", fields{createTestFields(), createTestData()}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, te.IsBinary(), "IsBinary()")
		})
	}
}

func TestTextRow_Length(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"Length", fields{createTestFields(), createTestData()}, 24},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			assert.Equalf(t, tt.want, te.Length(), "Length()")
		})
	}
}

func TestTextRow_Scan(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	type args struct {
		dest []proto.Value
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"Scan",
			fields{createTestFields(), []byte{0x01, 0x31, 0x05, 0x73, 0x63, 0x6f, 0x74, 0x74, 0x01, 0x31}},
			args{make([]proto.Value, 3)},
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			tt.wantErr(t, te.Scan(tt.args.dest), fmt.Sprintf("Scan(%v)", tt.args.dest))
		})
	}
}

func TestTextRow_WriteTo(t *testing.T) {
	type fields struct {
		fields []proto.Field
		raw    []byte
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   string
		wantN   int64
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"WriteTo",
			fields{createTestFields(), []byte{0x01, 0x31, 0x05, 0x73, 0x63, 0x6f, 0x74, 0x74, 0x01, 0x31}},
			"\x011\x05scott\x011",
			10,
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			te := TextRow{
				fields: tt.fields.fields,
				raw:    tt.fields.raw,
			}
			w := &bytes.Buffer{}
			gotN, err := te.WriteTo(w)
			if !tt.wantErr(t, err, fmt.Sprintf("WriteTo(%v)", w)) {
				return
			}
			assert.Equalf(t, tt.wantW, w.String(), "WriteTo(%v)", w)
			assert.Equalf(t, tt.wantN, gotN, "WriteTo(%v)", w)
		})
	}
}

func createTestFields() []proto.Field {
	result := []proto.Field{
		NewField("id", consts.FieldTypeLongLong),
		NewField("name", consts.FieldTypeString),
		NewField("gender", consts.FieldTypeLongLong),
	}
	return result
}

func createTestData() []byte {
	return []byte{0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x73, 0x63, 0x6f, 0x74, 0x74, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
}
