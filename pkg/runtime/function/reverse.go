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
	"github.com/arana-db/arana/pkg/proto"
)

const FuncReverse = "REVERSE"

var _ proto.Func = (*reverseFunc)(nil)

func init() {
	proto.RegisterFunc(FuncReverse, reverseFunc{})
}

type reverseFunc struct{}

func reverseString(s string) string {
	runes := []rune(s)
	for from, to := 0, len(runes)-1; from < to; from, to = from+1, to-1 {
		runes[from], runes[to] = runes[to], runes[from]
	}
	return string(runes)
}

func (a reverseFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	str, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}

	if str == nil {
		return nil, nil
	}

	return proto.NewValueString(reverseString(str.String())), nil
}

func (a reverseFunc) NumInput() int {
	return 1
}
