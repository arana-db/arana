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
	"math"
	"strconv"
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
	// Value(posV/lengthV) -> int(pos/length), error:
	// Value(10) -> 10,nil
	// Value("10") -> 10,nil
	// Value(4.2) -> 4,nil    	Value(4.7) -> 5,nil
	// Value("2.1") -> 2,nil    Value("2.6") -> 3,nil
	// Value(true) -> 1,nil		Value(false) -> 0,nil
	// Value("xxq") -> 0,error	Value("1w") -> 0,error
	tryParse := func(argV proto.Value) (int, error) {
		arg, err := strconv.Atoi(argV.String())
		if err != nil {
			arg2, err := strconv.ParseFloat(argV.String(), 64)
			if err != nil {
				return 0, err
			}
			arg = int(math.Floor(arg2 + 0.5))
		}
		return arg, nil
	}

	// str
	strV, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}
	// pos
	posV, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}
	if strV == nil || posV == nil {
		return nil, nil
	}
	str := strV.String()
	pos, err := tryParse(posV)
	// Thanks to Mulavar, the last 2 case is checking the abs(pos) is greater than len(str)
	// In that case, return empty string
	if err != nil || pos == 0 || pos > len(str) || -1*pos > len(str) {
		return proto.NewValueString(""), nil
	}

	// SUBSTRING(str, pos)
	if len(inputs) == 2 {
		if pos > 0 {
			return proto.NewValueString(str[pos-1:]), nil
		} else {
			return proto.NewValueString(str[len(str)+pos:]), nil
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
	length, err := tryParse(lengthV)
	if err != nil {
		return proto.NewValueString(""), nil
	}
	if pos > 0 {
		return proto.NewValueString(str[pos-1 : pos-1+length]), nil
	} else {
		return proto.NewValueString(str[len(str)+pos : len(str)+pos+length]), nil
	}
}

func (a substringFunc) NumInput() int {
	return 2
}
