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
)

import (
	gxbig "github.com/dubbogo/gost/math/big"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/runes"
)

// FuncRpad is  https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_rpad
const FuncRpad = "RPAD"

var _ proto.Func = (*rpadFunc)(nil)

func init() {
	proto.RegisterFunc(FuncRpad, rpadFunc{})
}

type rpadFunc struct{}

func (r rpadFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d2, _ := gxbig.NewDecFromString(fmt.Sprint(val2))
	if d2.IsNegative() {
		return nil, nil
	}
	val3, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	num, err := d2.ToInt()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	result, err := r.getResult(runes.ConvertToRune(val1), num, runes.ConvertToRune(val3))
	return result, err
}

func (r rpadFunc) NumInput() int {
	return 3
}

func (r rpadFunc) getResult(runesfirst []rune, num int64, runessecond []rune) (string, error) {
	if num == 0 || len(runessecond) == 0 {
		return "", nil
	}
	if num < int64(len(runesfirst)) {
		return string(runesfirst[:num]), nil
	} else if num == int64(len(runesfirst)) {
		return string(runesfirst), nil
	} else {
		for {
			if num <= int64(len(runesfirst)) {
				break
			}
			runesfirst = append(runesfirst, runessecond...)
		}
		return string(runesfirst[:num]), nil
	}
}
