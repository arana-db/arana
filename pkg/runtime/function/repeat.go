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
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const FuncRepeat = "REPEAT"

var _ proto.Func = (*repeatFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRepeat, repeatFunc{})
}

type repeatFunc struct{}

func (a repeatFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	str, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncRepeat)
	}

	val, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncRepeat)
	}

	if str == nil || val == nil {
		return nil, nil
	}

	n, err := val.Int64()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot eval %s", FuncRepeat)
	}

	result := strings.Repeat(str.String(), int(n))

	return proto.NewValueString(result), nil
}

func (a repeatFunc) NumInput() int {
	return 2
}
