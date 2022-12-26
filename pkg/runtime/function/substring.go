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

// FuncSubstring is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_substring
const FuncSubstring = "SUBSTRING"

var _ proto.Func = (*substringFunc)(nil)

func init() {
	proto.RegisterFunc(FuncSubstring, substringFunc{})
}

type substringFunc struct{}

func (a substringFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	strV, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}
	posV, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}
	if strV == nil || posV == nil {
		return nil, nil
	}

	str := strV.String()
	strLength := int64(len(str))
	pos, err := posV.Int64()
	if err != nil || pos == 0 || pos > strLength || -pos > strLength {
		return proto.NewValueString(""), nil
	}

	// SUBSTRING(str, pos)
	if len(inputs) == 2 {
		if pos > 0 {
			return proto.NewValueString(str[pos-1:]), nil
		} else {
			return proto.NewValueString(str[strLength+pos:]), nil
		}
	}

	// SUBSTRING(str, pos, length)
	lengthV, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, err
	}
	if lengthV == nil {
		return nil, nil
	}
	length, err := lengthV.Int64()
	if err != nil {
		return proto.NewValueString(""), nil
	}
	if pos > 0 {
		return proto.NewValueString(str[pos-1 : pos-1+length]), nil
	} else {
		return proto.NewValueString(str[strLength+pos : strLength+pos+length]), nil
	}
}

func (a substringFunc) NumInput() int {
	return 2
}
