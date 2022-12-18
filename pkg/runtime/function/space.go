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

const FuncSpace = "SPACE"

var _ proto.Func = (*spaceFunc)(nil)

func init() {
	proto.RegisterFunc(FuncSpace, spaceFunc{})
}

type spaceFunc struct{}

func (c spaceFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var res strings.Builder
	switch v := val.(type) {
	case uint8:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case uint16:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case uint32:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case uint64:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case uint:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case int64:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case int32:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case int16:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case int8:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	case int:
		for v > 0 {
			res.WriteString(" ")
			v--
		}
		return res.String(), nil
	default:
		return "NaN", nil
	}
}

func (c spaceFunc) NumInput() int {
	return 1
}
