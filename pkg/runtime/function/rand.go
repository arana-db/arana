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
	"math/rand"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const FuncRand = "RAND"

var _ proto.Func = (*randFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRand, randFunc{})
}

type randFunc struct{}

// Apply rand func
func (c randFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	if len(inputs) < 1 {
		return proto.NewValueFloat64(rand.Float64()), nil
	}

	var seed int64
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if val != nil {
		seed, _ = val.Int64()
	}

	return c.rand(seed), nil
}

// rand Returns a random floating-point value v in the range 0 <= v < 1.0
func (c randFunc) rand(value int64) proto.Value {
	r := rand.New(rand.NewSource(value))
	return proto.NewValueFloat64(r.Float64())
}

func (c randFunc) NumInput() int {
	return 0
}
