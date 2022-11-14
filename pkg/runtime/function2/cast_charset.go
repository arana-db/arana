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

package function2

import (
	"context"
	"fmt"
	"strings"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/charsetconv"
)

// FuncCastCharset FuncCastNchar is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastCharset = "CAST_CHARSET"

var _ proto.Func = (*castcharsetFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastCharset, castcharsetFunc{})
}

type castcharsetFunc struct{}

func (a castcharsetFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {

	if len(inputs) != 2 {
		return "", errors.New("The Charset function must accept two parameters\n")
	}

	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	inCharset := fmt.Sprint(val1)
	inCharset = strings.TrimSpace(inCharset)
	inSrc := fmt.Sprint(val2)
	inSrc = strings.TrimSpace(inSrc)

	// decode: inSrc to utf8 encode-type utf8Src
	utf8_charset, err := charsetconv.GetCharset("utf8")
	if err != nil {
		return "", err
	}
	utf8Src, err := charsetconv.Decode(inSrc, utf8_charset)
	if err != nil {
		return "", err
	}
	// encode: utf8Src to special encode-type res
	in_charset, err := charsetconv.GetCharset(inCharset)
	if err != nil {
		return "", err
	}
	res, err := charsetconv.Encode(utf8Src, in_charset)
	if err != nil {
		return "", err
	}
	return res, nil

}

func (a castcharsetFunc) NumInput() int {
	return 2
}
