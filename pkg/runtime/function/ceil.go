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
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncCeil is https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_ceil
const FuncCeil = "CEIL"

var _ proto.Func = (*ceilFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCeil, ceilFunc{})
}

type ceilFunc struct{}

func (c ceilFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncCeil)
	}

	if val == nil {
		return nil, nil
	}

	d, err := val.Decimal()
	if err != nil {
		return proto.NewValueFloat64(0), nil
	}

	return proto.NewValueInt64(d.Ceil().IntPart()), nil
}

func (c ceilFunc) NumInput() int {
	return 1
}
