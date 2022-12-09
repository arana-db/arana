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
	"strings"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/runtime/ast"
)

// FuncReplace is https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_replace
const FuncReplace = "REPLACE"

var _ proto.Func = (*replaceFunc)(nil)

func init() {
	proto.RegisterFunc(FuncReplace, replaceFunc{})
}

type replaceFunc struct{}

func (c replaceFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// arg0
	str, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, err
	}
	// arg1
	fromStr, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, err
	}
	// arg2
	toStr, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, err
	}
	// if a NULL exists, directly return `NULL`
	isNull := func(val any) bool {
		_, ok := val.(ast.Null)
		return ok
	}
	if isNull(str) || isNull(fromStr) || isNull(toStr) {
		return ast.Null{}, nil
	}
	// if args' type is bool, should be converted to `0` or `1` in advanced
	// else, convert to string
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
	strS := arg2String(str)
	fromStrS := arg2String(fromStr)
	toStrS := arg2String(toStr)
	// do replace
	ret := strings.ReplaceAll(strS, fromStrS, toStrS)
	return ret, nil
}

func (c replaceFunc) NumInput() int {
	return 3
}
