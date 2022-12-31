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

	if str == nil || fromStr == nil || toStr == nil {
		return nil, nil
	}

	s := strings.ReplaceAll(str.String(), fromStr.String(), toStr.String())
	return proto.NewValueString(s), nil
}

func (c replaceFunc) NumInput() int {
	return 3
}
