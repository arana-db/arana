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
	"strings"
)

import (
	"github.com/pkg/errors"
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
	var strS, fromStrS, toStrS string
	// arg0
	str, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// arg1
	fromStr, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// arg2
	toStr, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// if a NULL exists, show return NULL
	isNull := func(val any) bool {
		_, ok := val.(ast.Null)
		return ok
	}
	if isNull(str) || isNull(fromStr) || isNull(toStr) {
		return ast.Null{}, nil
	}
	// if a Number type, convert to string firstly
	isNumber := func(val any) bool {
		_, ok := val.(int)
		return ok
	}
	if isNumber(str) {
		strS = strconv.Itoa(str.(int))
	} else {
		strS = str.(string)
	}
	if isNumber(fromStr) {
		fromStrS = strconv.Itoa(fromStr.(int))
	} else {
		fromStrS = fromStr.(string)
	}
	if isNumber(toStr) {
		toStrS = strconv.Itoa(toStr.(int))
	} else {
		toStrS = toStr.(string)
	}
	// do replace
	ret := strings.ReplaceAll(strS, fromStrS, toStrS)
	return ret, nil
}

func (c replaceFunc) NumInput() int {
	return 3
}
