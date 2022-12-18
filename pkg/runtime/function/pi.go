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
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncPi is https://dev.mysql.com/doc/refman/5.6/en/mathematical-functions.html#function_pi
const FuncPi = "PI"

const _piDefault = 3.141593

var _ proto.Func = (*piFunc)(nil)

func init() {
	proto.RegisterFunc(FuncPi, piFunc{})
}

type piFunc struct{}

func (p piFunc) Apply(_ context.Context, _ ...proto.Valuer) (proto.Value, error) {
	return proto.NewValueFloat64(_piDefault), nil
}

func (p piFunc) NumInput() int {
	return 0
}
