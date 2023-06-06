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

package dataset

import (
	"fmt"
	"io"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	constants "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

func FieldsAll() []proto.Field {
	return []proto.Field{
		mysql.NewField("id", constants.FieldTypeLong),
		mysql.NewField("name", constants.FieldTypeVarChar),
		mysql.NewField("gender", constants.FieldTypeLong),
		mysql.NewField("id", constants.FieldTypeLong),
		mysql.NewField("name", constants.FieldTypeVarChar),
		mysql.NewField("gender", constants.FieldTypeLong),
	}
}

func Field() []proto.Field {
	return []proto.Field{
		mysql.NewField("id", constants.FieldTypeLong),
		mysql.NewField("name", constants.FieldTypeVarChar),
		mysql.NewField("gender", constants.FieldTypeLong),
	}
}

func testCase1() (*VirtualDataset, *VirtualDataset) {
	field := Field()
	outer := &VirtualDataset{
		Columns: field,
	}

	for i := int64(0); i < 10; i++ {
		outer.Rows = append(outer.Rows, rows.NewTextVirtualRow(field, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	inner := &VirtualDataset{
		Columns: field,
	}

	for i := int64(5); i < 15; i++ {
		inner.Rows = append(inner.Rows, rows.NewTextVirtualRow(field, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	return outer, inner
}

func testCase2() (*VirtualDataset, *VirtualDataset) {
	field := Field()

	outer := &VirtualDataset{
		Columns: field,
	}

	outer.Rows = append(outer.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(2),
		proto.NewValueString(fmt.Sprintf("fake-name-%d", 2)),
		proto.NewValueInt64(0), // 0=female,1=male
	}))

	outer.Rows = append(outer.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(2),
		proto.NewValueString(fmt.Sprintf("fake-name-%f", 2.1)),
		proto.NewValueInt64(1), // 0=female,1=male
	}))

	outer.Rows = append(outer.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(3),
		proto.NewValueString(fmt.Sprintf("fake-name-%f", 2.2)),
		proto.NewValueInt64(1), // 0=female,1=male
	}))

	inner := &VirtualDataset{
		Columns: field,
	}

	inner.Rows = append(inner.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(2),
		proto.NewValueString(fmt.Sprintf("fake-name-%d", 3)),
		proto.NewValueInt64(0), // 0=female,1=male
	}))

	inner.Rows = append(inner.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(2),
		proto.NewValueString(fmt.Sprintf("fake-name-%f", 3.1)),
		proto.NewValueInt64(1), // 0=female,1=male
	}))

	inner.Rows = append(inner.Rows, rows.NewTextVirtualRow(field, []proto.Value{
		proto.NewValueInt64(4),
		proto.NewValueString(fmt.Sprintf("fake-name-%f", 3.2)),
		proto.NewValueInt64(0), // 0=female,1=male
	}))

	return outer, inner
}

func testCase3() (*VirtualDataset, *VirtualDataset) {
	field := Field()

	outer := &VirtualDataset{
		Columns: field,
	}

	for i := int64(1); i < 4; i++ {
		outer.Rows = append(outer.Rows, rows.NewTextVirtualRow(field, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	inner := &VirtualDataset{
		Columns: field,
	}

	for i := int64(4); i < 7; i++ {
		inner.Rows = append(inner.Rows, rows.NewTextVirtualRow(field, []proto.Value{
			proto.NewValueInt64(i),
			proto.NewValueString(fmt.Sprintf("fake-name-%d", i)),
			proto.NewValueInt64(i & 1), // 0=female,1=male
		}))
	}

	return outer, inner
}

func TestInnerSortMergeJoin_Next(t *testing.T) {
	fieldsAll := FieldsAll()
	// test case 1: outer value [0,1,2,3,4,5,6,7,8,9], inner value [5,6,7,8,9,10,11,12,13,14]
	outer1, inner1 := testCase1()

	// test case 2: outer value [2,2,3], inner value [2,2,4]
	outer2, inner2 := testCase2()

	// test case 3: outer value [1,2,3], inner value [4,5,6]
	outer3, inner3 := testCase3()

	type fields struct {
		fields     []proto.Field
		joinColumn *JoinColumn
		joinType   ast.JoinType
		outer      proto.Dataset
		inner      proto.Dataset
		beforeRow  proto.Row
	}
	tests := []struct {
		name    string
		fields  fields
		want    proto.Row
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test case inner join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.InnerJoin,
				outer:    outer1,
				inner:    inner1,
			},
		},
		{
			name: "test case inner join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.InnerJoin,
				outer:    outer2,
				inner:    inner2,
			},
		},
		{
			name: "test case inner join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.InnerJoin,
				outer:    outer3,
				inner:    inner3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := NewSortMergeJoin(tt.fields.joinType, tt.fields.joinColumn, tt.fields.outer, tt.fields.inner)
			for {
				row, err := s.Next()
				if err == io.EOF {
					return
				}

				if row != nil {
					dest := make([]proto.Value, len(fieldsAll))
					err = row.Scan(dest)
					assert.NoError(t, err)
					t.Logf("id=%v, name=%v, gender=%v, id1=%v, name1=%v, gender1=%v\n", dest[0], dest[1], dest[2], dest[3], dest[4], dest[5])
				}
			}
		})
	}
}

func TestLeftSortMergeJoin_Next(t *testing.T) {
	fieldsAll := FieldsAll()

	// test case 1: outer value [0,1,2,3,4,5,6,7,8,9], inner value [5,6,7,8,9,10,11,12,13,14]
	outer1, inner1 := testCase1()

	// test case 2: outer value [2,2,3], inner value [2,2,4]
	outer2, inner2 := testCase2()

	// test case 3: outer value [1,2,3], inner value [4,5,6]
	outer3, inner3 := testCase3()

	type fields struct {
		fields     []proto.Field
		joinColumn *JoinColumn
		joinType   ast.JoinType
		outer      proto.Dataset
		inner      proto.Dataset
		beforeRow  proto.Row
	}
	tests := []struct {
		name    string
		fields  fields
		want    proto.Row
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test case left join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.LeftJoin,
				outer:    outer1,
				inner:    inner1,
			},
		},
		{
			name: "test case left join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.LeftJoin,
				outer:    outer2,
				inner:    inner2,
			},
		},
		{
			name: "test case left join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.LeftJoin,
				outer:    outer3,
				inner:    inner3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := NewSortMergeJoin(tt.fields.joinType, tt.fields.joinColumn, tt.fields.outer, tt.fields.inner)
			for {
				row, err := s.Next()
				if err == io.EOF {
					return
				}

				if row != nil {
					dest := make([]proto.Value, len(fieldsAll))
					err = row.Scan(dest)
					assert.NoError(t, err)
					t.Logf("id=%v, name=%v, gender=%v, id1=%v, name1=%v, gender1=%v\n", dest[0], dest[1], dest[2], dest[3], dest[4], dest[5])
				}
			}
		})
	}
}

func TestRightSortMergeJoin_Next(t *testing.T) {
	fieldsAll := FieldsAll()

	// test case 1: outer value [0,1,2,3,4,5,6,7,8,9], inner value [5,6,7,8,9,10,11,12,13,14]
	outer1, inner1 := testCase1()

	// test case 2: outer value [2,2,3], inner value [2,2,4]
	outer2, inner2 := testCase2()

	// test case 3: outer value [1,2,3], inner value [4,5,6]
	outer3, inner3 := testCase3()

	type fields struct {
		fields     []proto.Field
		joinColumn *JoinColumn
		joinType   ast.JoinType
		outer      proto.Dataset
		inner      proto.Dataset
		beforeRow  proto.Row
	}
	tests := []struct {
		name    string
		fields  fields
		want    proto.Row
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "test case right join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.RightJoin,
				outer:    outer1,
				inner:    inner1,
			},
		},
		{
			name: "test case right join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.RightJoin,
				outer:    outer2,
				inner:    inner2,
			},
		},
		{
			name: "test case right join",
			fields: fields{
				fields: fieldsAll,
				joinColumn: &JoinColumn{
					column: "id",
				},
				joinType: ast.RightJoin,
				outer:    outer3,
				inner:    inner3,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := NewSortMergeJoin(tt.fields.joinType, tt.fields.joinColumn, tt.fields.outer, tt.fields.inner)
			for {
				row, err := s.Next()
				if err == io.EOF {
					return
				}

				if row != nil {
					dest := make([]proto.Value, len(fieldsAll))
					err = row.Scan(dest)
					assert.NoError(t, err)
					t.Logf("id=%v, name=%v, gender=%v, id1=%v, name1=%v, gender1=%v\n", dest[0], dest[1], dest[2], dest[3], dest[4], dest[5])
				}
			}
		})
	}
}
