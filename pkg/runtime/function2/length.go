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
	"strings"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const FuncLength = "LENGTH"

var _ proto.Func = (*lengthFunc)(nil)

func init() {
	proto.RegisterFunc(FuncLength, lengthFunc{})
}

type lengthFunc struct{}

func (c lengthFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	decLength := func(v interface{}) int {
		return strings.Count(fmt.Sprint(v), "") - 1
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decLength(v.Value), nil
	default:
		return decLength(v), nil
	}
}

func (c lengthFunc) NumInput() int {
	return 1
}
