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
	"crypto/md5"
	"fmt"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

const FuncMd5 = "MD5"

var _ proto.Func = (*md5Func)(nil)

func init() {
	proto.RegisterFunc(FuncMd5, md5Func{})
}

type md5Func struct{}

func (c md5Func) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	decMd5 := func(v interface{}) string {
		return fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprint(v))))
	}

	switch v := val.(type) {
	case *gxbig.Decimal:
		return decMd5(v.Value), nil
	default:
		return decMd5(v), nil
	}
}

func (c md5Func) NumInput() int {
	return 1
}
