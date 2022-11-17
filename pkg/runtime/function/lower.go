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

package function

import (
	"context"
	"fmt"
	"strings"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncLower is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lower
const FuncLower = "LOWER"

var _ proto.Func = (*lowerFunc)(nil)

func init() {
	proto.RegisterFunc(FuncLower, lowerFunc{})
}

type lowerFunc struct{}

func (c lowerFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	switch v := val.(type) {
	case uint8, uint16, uint32, uint64, uint, int64, int32, int16, int8, int, float64, float32, *gxbig.Decimal:
		return v, nil
	default:
		return strings.ToLower(fmt.Sprint(v)), nil
	}
}

func (c lowerFunc) NumInput() int {
	return 1
}
