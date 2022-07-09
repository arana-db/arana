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
	"database/sql"
	"io"
	"math"
	"sync"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/dataset"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

type combinerManager struct {
}

type (
	Combiner interface {
		Merge(result proto.Result, loader *AggrLoader) (proto.Result, error)
	}
)

func (c combinerManager) Merge(result proto.Result, loader *AggrLoader) (proto.Result, error) {
	if len(loader.Aggrs) < 1 {
		return result, nil
	}

	ds, err := result.Dataset()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	defer func() {
		_ = ds.Close()
	}()

	mergeVals := make([]proto.Value, 0, len(loader.Aggrs))
	for i := 0; i < len(loader.Aggrs); i++ {
		switch loader.Aggrs[i] {
		case ast.AggrAvg:
			mergeVals = append(mergeVals, gxbig.NewDecFromInt(0))
		case ast.AggrMin:
			mergeVals = append(mergeVals, gxbig.NewDecFromInt(math.MaxInt64))
		case ast.AggrMax:
			mergeVals = append(mergeVals, gxbig.NewDecFromInt(math.MinInt64))
		default:
			mergeVals = append(mergeVals, gxbig.NewDecFromInt(0))
		}
	}

	var (
		row       proto.Row
		fields, _ = ds.Fields()
		vals      = make([]proto.Value, len(fields))

		isBinary     bool
		isBinaryOnce sync.Once
	)

	for {
		row, err = ds.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "failed to aggregate values")
		}

		if err = row.Scan(vals); err != nil {
			return nil, errors.Wrap(err, "failed to aggregate values")
		}

		isBinaryOnce.Do(func() {
			isBinary = row.IsBinary()
		})

		for aggrIdx := range loader.Aggrs {
			dummyVal := mergeVals[aggrIdx].(*gxbig.Decimal)
			var (
				s            sql.NullString
				floatDecimal *gxbig.Decimal
			)
			_ = s.Scan(vals[aggrIdx])

			if !s.Valid {
				continue
			}

			if floatDecimal, err = gxbig.NewDecFromString(s.String); err != nil {
				return nil, errors.WithStack(err)
			}

			switch loader.Aggrs[aggrIdx] {
			case ast.AggrMax:
				if dummyVal.Compare(floatDecimal) < 0 {
					dummyVal = floatDecimal
				}
			case ast.AggrMin:
				if dummyVal.Compare(floatDecimal) > 0 {
					dummyVal = floatDecimal
				}
			case ast.AggrSum, ast.AggrCount:
				_ = gxbig.DecimalAdd(dummyVal, floatDecimal, dummyVal)
			}
			mergeVals[aggrIdx] = dummyVal
		}
	}

	for aggrIdx := range loader.Aggrs {
		val := mergeVals[aggrIdx].(*gxbig.Decimal)
		mergeVals[aggrIdx] = val
	}

	ret := &dataset.VirtualDataset{
		Columns: fields,
	}

	if isBinary {
		ret.Rows = append(ret.Rows, rows.NewBinaryVirtualRow(fields, mergeVals))
	} else {
		ret.Rows = append(ret.Rows, rows.NewTextVirtualRow(fields, mergeVals))
	}

	return resultx.New(resultx.WithDataset(ret)), nil
}

func NewCombinerManager() Combiner {
	return combinerManager{}
}
