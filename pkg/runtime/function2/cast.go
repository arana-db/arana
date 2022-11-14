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
)

import (
	"github.com/arana-db/arana/pkg/proto"

	"github.com/pkg/errors"
)

const FuncCast = "CAST"

var _ proto.Func = (*castFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCast, castFunc{})
}

type castFunc struct{}

func (c castFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	signCast := func(i uint) *int {
		var sign int
		return &sign
	}

	unSignCast := func(i any) any {
		switch v := i.(type) {
		case int:
			if v > 0 {
				return uint(v)
			}
			return 0
		case int8:
			if v > 0 {
				return uint8(v)
			}
			return 0
		case int16:
			if v > 0 {
				return uint16(v)
			}
			return 0
		case int32:
			if v > 0 {
				return uint32(v)
			}
			return 0
		case int64:
			if v > 0 {
				return uint64(v)
			}
			return 0
		default:
			return 0
		}
	}

	switch v := val.(type) {
	case uint8, uint16, uint32, uint64:
		return v, nil
	case uint:
		return signCast(v), nil
	case int, int8, int16, int32, int64:
		return unSignCast(v), nil
	default:
		return v, nil
	}
}

func (c castFunc) NumInput() int {
	return 1
}
