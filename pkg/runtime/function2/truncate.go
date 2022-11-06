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

package function2

import (
	"context"
	"fmt"
	"math"
	"strconv"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncTruncate is https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_truncate
const FuncTruncate = "TRUNCATE"

var _ proto.Func = (*truncateFunc)(nil)

func init() {
	proto.RegisterFunc(FuncTruncate, truncateFunc{})
}

type truncateFunc struct{}

func (t truncateFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	x, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	v, err := strconv.ParseFloat(fmt.Sprint(x), 64)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	e, err := strconv.Atoi(fmt.Sprint(d))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	res := math.Trunc(v*math.Pow10(e)) * math.Pow10(-e)

	switch x.(type) {
	case uint8:
		return uint8(res), nil
	case uint16:
		return uint16(res), nil
	case uint32:
		return uint32(res), nil
	case uint64:
		return uint64(res), nil
	case uint:
		return uint(res), nil
	case int8:
		return int8(res), nil
	case int16:
		return int16(res), nil
	case int32:
		return int32(res), nil
	case int64:
		return int64(res), nil
	case int:
		return int(res), nil
	case float64:
		return res, nil
	case float32:
		return float32(res), nil
	default:
		return gxbig.NewDecFromFloat(res)
	}
}

func (t truncateFunc) NumInput() int {
	return 2
}
