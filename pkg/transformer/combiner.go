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

package transformer

import (
	"io"
	"math"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	mysql2 "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/proto"
	ast2 "github.com/arana-db/arana/pkg/runtime/ast"
)

type combinerManager struct {
}

type (
	Combiner interface {
		Merge(result proto.Result, loader *AggrLoader) (proto.Result, error)
	}
)

func (c combinerManager) Merge(result proto.Result, loader *AggrLoader) (proto.Result, error) {
	if closer, ok := result.(io.Closer); ok {
		defer func() {
			_ = closer.Close()
		}()
	}

	result = &mysql.Result{
		Fields:   result.GetFields(),
		Rows:     result.GetRows(),
		DataChan: make(chan proto.Row, 1),
	}

	if len(loader.Aggrs) < 1 {
		return result, nil
	}

	rows := result.GetRows()
	rowsLen := len(rows)
	if rowsLen < 1 {
		return result, nil
	}

	mergeRows := make([]proto.Row, 0, 1)
	mergeVals := make([]*proto.Value, 0, len(loader.Aggrs))
	for i := 0; i < len(loader.Aggrs); i++ {
		switch loader.Aggrs[i][0] {
		case ast2.AggrAvg:
			mergeVals = append(mergeVals, &proto.Value{Typ: mysql2.FieldTypeDecimal, Val: gxbig.NewDecFromInt(0), Len: 8})
		case ast2.AggrMin:
			mergeVals = append(mergeVals, &proto.Value{Typ: mysql2.FieldTypeDecimal, Val: gxbig.NewDecFromInt(math.MaxInt64), Len: 8})
		case ast2.AggrMax:
			mergeVals = append(mergeVals, &proto.Value{Typ: mysql2.FieldTypeDecimal, Val: gxbig.NewDecFromInt(math.MinInt64), Len: 8})
		default:
			mergeVals = append(mergeVals, &proto.Value{Typ: mysql2.FieldTypeLongLong, Val: gxbig.NewDecFromInt(0), Len: 8})
		}
	}

	for _, row := range rows {
		tRow := &mysql.TextRow{
			Row: mysql.Row{
				Content: row.Data(),
				ResultSet: &mysql.ResultSet{
					Columns:     row.Fields(),
					ColumnNames: row.Columns(),
				},
			},
		}
		vals, err := tRow.Decode()
		if err != nil {
			return result, errors.WithStack(err)
		}

		if vals == nil {
			continue
		}

		for aggrIdx := range loader.Aggrs {
			dummyVal := mergeVals[aggrIdx].Val.(*gxbig.Decimal)
			switch loader.Aggrs[aggrIdx][0] {
			case ast2.AggrMax:
				if v, ok := vals[aggrIdx].Val.([]uint8); ok {
					floatDecimal, err := gxbig.NewDecFromString(string(v))
					if err != nil {
						return nil, errors.WithStack(err)
					}
					if dummyVal.Compare(floatDecimal) < 0 {
						dummyVal = floatDecimal
					}
				}
			case ast2.AggrMin:
				if v, ok := vals[aggrIdx].Val.([]uint8); ok {
					floatDecimal, err := gxbig.NewDecFromString(string(v))
					if err != nil {
						return nil, errors.WithStack(err)
					}
					if dummyVal.Compare(floatDecimal) > 0 {
						dummyVal = floatDecimal
					}
				}
			case ast2.AggrSum, ast2.AggrCount:
				if v, ok := vals[aggrIdx].Val.([]uint8); ok {
					floatDecimal, err := gxbig.NewDecFromString(string(v))
					if err != nil {
						return nil, errors.WithStack(err)
					}
					gxbig.DecimalAdd(dummyVal, floatDecimal, dummyVal)
				}
			}
			mergeVals[aggrIdx].Val = dummyVal
		}
	}

	for aggrIdx := range loader.Aggrs {
		val := mergeVals[aggrIdx].Val.(*gxbig.Decimal)
		mergeVals[aggrIdx].Val, _ = val.ToFloat64()
		mergeVals[aggrIdx].Raw = []byte(val.String())
		mergeVals[aggrIdx].Len = len(mergeVals[aggrIdx].Raw)
	}
	r := &mysql.TextRow{}
	row := r.Encode(mergeVals, result.GetFields(), loader.Alias).(*mysql.TextRow)
	mergeRows = append(mergeRows, &row.Row)

	return &mysql.Result{
		Fields:       result.GetFields(),
		Rows:         mergeRows,
		AffectedRows: 1,
		InsertId:     0,
		DataChan:     make(chan proto.Row, 1),
	}, nil
}

func NewCombinerManager() Combiner {
	return combinerManager{}
}
