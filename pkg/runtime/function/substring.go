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
	"fmt"
	"strconv"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

// FuncSubstring is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_substring
const FuncSubstring = "SUBSTRING"

var _ proto.Func = (*substringFunc)(nil)

func init() {
	proto.RegisterFunc(FuncSubstring, substringFunc{})
}

type substringFunc struct{}

func (a substringFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// judge if `val` is `NULL`
	isNull := func(val any) bool {
		_, ok := val.(ast.Null)
		return ok
	}
	// if args' type is bool, should be converted to `0` or `1` in advanced
	arg2String := func(arg proto.Value) string {
		boolVal, isToStrBool := arg.(bool)
		if isToStrBool {
			if boolVal {
				return "1"
			} else {
				return "0"
			}
		} else {
			return fmt.Sprint(arg)
		}
	}

	// arg0: str
	strV, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}
	// arg1: pos
	posV, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}
	// if any arg is `NULL`, return `NULL`
	if isNull(strV) || isNull(posV) {
		return ast.Null{}, nil
	}
	str := arg2String(strV)
	pos, err := strconv.Atoi(arg2String(posV))
	if err == strconv.ErrSyntax {
		// if `pos`,`len` is string but not a number string,(such as "w","e"...)
		// return empty string
		return "", nil
	}
	// `SUBSTRING(str, pos)`
	if len(inputs) == 2 {
		// cut string
		if pos > 0 {
			return str[pos-1:], nil
		} else if pos < 0 {
			return str[len(str)+pos:], nil
		} else {
			return "", nil
		}
	}

	// `SUBSTRING(str, pos, len)`
	// arg2[optional]: `len`
	lenV, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, err
	}
	if isNull(lenV) {
		return ast.Null{}, nil
	}
	length, err := strconv.Atoi(arg2String(lenV))
	if err == strconv.ErrSyntax || length < 1 {
		// if `pos`,`len` is a string but not a number string,(such as "w","e"...)
		// if len < 1,
		// return empty string
		return "", nil
	}
	// cut string
	if pos > 0 {
		return str[pos-1 : pos-1+length], nil
	} else if pos < 0 {
		return str[len(str)+pos : len(str)+pos+length], nil
	} else {
		return "", nil
	}
}

func (a substringFunc) NumInput() int {
	return 2
}
