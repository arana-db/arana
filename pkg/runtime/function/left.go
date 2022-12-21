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
	"github.com/arana-db/arana/pkg/util/runes"
)

// FuncLeft is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_left
const FuncLeft = "LEFT"

var _ proto.Func = (*leftFunc)(nil)

func init() {
	proto.RegisterFunc(FuncLeft, leftFunc{})
}

type leftFunc struct{}

func (c leftFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	strInput, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	lenInput, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if strInput == nil || lenInput == nil {
		return nil, nil
	}

	strStr := strInput.String()
	num, _ := lenInput.Int64()

	if num < 1 {
		return proto.NewValueString(""), nil
	}

	s, err := getLeftChar(runes.ConvertToRune(strStr), num)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return proto.NewValueString(s), nil
}

func (c leftFunc) NumInput() int {
	return 2
}

func getLeftChar(runes []rune, num int64) (string, error) {
	if num > int64(len(runes)) {
		return string(runes), nil
	} else if num >= 0 {
		return string(runes[:num]), nil
	}
	return "", nil
}
