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

	"github.com/arana-db/arana/pkg/proto"
	"github.com/pkg/errors"
)

// FuncIfNull is https://dev.mysql.com/doc/refman/5.6/en/flow-control-functions.html#function_ifnull
const FuncIfNull = "IFNULL"

var _ proto.Func = (*ifNullFunc)(nil)

func init() {
	proto.RegisterFunc(FuncIfNull, ifNullFunc{})
}

type ifNullFunc struct{}

func (i ifNullFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if val1 != nil {
		return val1, nil
	} else {
		return val2, nil
	}
}

func (i ifNullFunc) NumInput() int {
	return 2
}
