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
	charset, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	content, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	inCharset := strings.TrimSpace(charset.String())
	inSrc := strings.TrimSpace(content.String())

	// decode: inSrc to utf8 encode-type utf8Src
	utf8c, err := charsetconv.GetCharset("utf8")
	if err != nil {
		return nil, err
	}
	utf8Src, err := charsetconv.Decode(inSrc, utf8c)
	if err != nil {
		return nil, err
	}
	// encode: utf8Src to special encode-type res
	c, err := charsetconv.GetCharset(inCharset)
	if err != nil {
		return nil, err
	}
	res, err := charsetconv.Encode(utf8Src, c)
	if err != nil {
		return nil, err
	}
	return proto.NewValueString(res), nil
}

func (a castcharsetFunc) NumInput() int {
	return 2
}
