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
	strV, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}
	posV, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}
	// if any arg is NULL, return NULL
	if strV == nil || posV == nil {
		return nil, nil
	}
	str := strV.String()
	pos, err := strconv.Atoi(posV.String())
	if err != nil {
		return proto.NewValueString(""), nil
	}

	// SUBSTRING(str, pos)
	if len(inputs) == 2 {
		// cut string
		if pos > 0 {
			return proto.NewValueString(str[pos-1:]), nil
		} else if pos < 0 {
			return proto.NewValueString(str[len(str)+pos:]), nil
		} else {
			return proto.NewValueString(""), nil
		}
	}

	// SUBSTRING(str, pos, length)
	lenV, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, err
	}
	// if any arg is NULL, return NULL
	if lenV == nil {
		return nil, nil
	}
	length, err := strconv.Atoi(lenV.String())
	if err != nil || length < 1 {
		return proto.NewValueString(""), nil
	}
	// cut string
	if pos > 0 {
		return proto.NewValueString(str[pos-1 : pos-1+length]), nil
	} else if pos < 0 {
		return proto.NewValueString(str[len(str)+pos : len(str)+pos+length]), nil
	} else {
		return proto.NewValueString(""), nil
	}
}

func (a substringFunc) NumInput() int {
	return 2
}
