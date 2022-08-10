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
	"database/sql"
	"io"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/reduce"
)

type ReduceDataset struct {
	proto.Dataset
	Reducers map[int]reduce.Reducer // field_index -> aggregator
	prev     []proto.Value
	binary   bool
	eof      bool
}

func (ad *ReduceDataset) Next() (proto.Row, error) {
	if ad.eof {
		return nil, io.EOF
	}

	nextRow, err := ad.Dataset.Next()
	if errors.Is(err, io.EOF) {
		if ad.prev == nil {
			return nil, io.EOF
		}

		ad.eof = true
		fields, _ := ad.Dataset.Fields()
		if ad.binary {
			return rows.NewBinaryVirtualRow(fields, ad.prev), nil
		} else {
			return rows.NewTextVirtualRow(fields, ad.prev), nil
		}
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}
	fields, _ := ad.Fields()
	values := make([]proto.Value, len(fields))
	if err = nextRow.Scan(values); err != nil {
		return nil, errors.WithStack(err)
	}

	if ad.prev == nil {
		ad.prev = values
		ad.binary = nextRow.IsBinary()
		return ad.Next()
	}

	var (
		prevValue, nextValue *gxbig.Decimal
		result               *gxbig.Decimal
	)
	for i := range values {
		red, ok := ad.Reducers[i]
		if !ok {
			continue
		}
		var (
			prev = ad.prev[i]
			next = values[i]
		)

		if next == nil {
			continue
		}
		if prev == nil {
			ad.prev[i] = next
			continue
		}

		if prevValue, err = toValue(fields[i], prev); err != nil {
			return nil, errors.WithStack(err)
		}
		if nextValue, err = toValue(fields[i], next); err != nil {
			return nil, errors.WithStack(err)
		}
		if result, err = red.Decimal(prevValue, nextValue); err != nil {
			return nil, errors.WithStack(err)
		}

		ad.prev[i] = result
	}

	return ad.Next()
}

func toValue(field proto.Field, input interface{}) (*gxbig.Decimal, error) {
	switch v := input.(type) {
	case *gxbig.Decimal:
		return v, nil
	case string:
		return gxbig.NewDecFromString(v)
	case int64:
		return gxbig.NewDecFromInt(v), nil
	}

	var s sql.NullString
	if err := s.Scan(input); err != nil {
		return nil, errors.WithStack(err)
	}
	return gxbig.NewDecFromString(s.String)
}
