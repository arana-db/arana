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
	"github.com/arana-db/arana/pkg/proto"
)

// FuncConcatWS https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_concat-ws
const FuncConcatWS = "CONCAT_WS"

var _ proto.Func = (*concatWSFunc)(nil)

func init() {
	proto.RegisterFunc(FuncConcatWS, concatWSFunc{})
}

type concatWSFunc struct{}

func (c concatWSFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	var (
		sb       strings.Builder
		sep, val proto.Value
		err      error
	)

	if sep, err = inputs[0].Value(ctx); err != nil {
		return nil, err
	}
	// check separator is NULL
	if sep == nil {
		return nil, nil
	}

	i := 1

	// 1. write the first not-null value in string
	for ; i < len(inputs); i++ {
		if val, err = inputs[i].Value(ctx); err != nil {
			return nil, err
		}
		if val != nil {
			sb.WriteString(val.String())
			break
		}
	}

	i++

	// 2. write other non-null value in string, includes sep
	for ; i < len(inputs); i++ {
		if val, err = inputs[i].Value(ctx); err != nil {
			return nil, err
		}

		if val == nil {
			continue
		}

		sb.WriteString(sep.String())
		sb.WriteString(val.String())
	}

	return proto.NewValueString(sb.String()), nil
}

func (c concatWSFunc) NumInput() int {
	return 2
}
