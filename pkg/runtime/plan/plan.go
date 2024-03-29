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

package plan

import (
	"go.opentelemetry.io/otel"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

var Tracer = otel.Tracer("ExecPlan")

type BasePlan struct {
	Args []proto.Value
}

func (bp *BasePlan) BindArgs(args []proto.Value) {
	bp.Args = args
}

func (bp *BasePlan) ToArgs(indexes []int) []proto.Value {
	if len(indexes) < 1 || len(bp.Args) < 1 {
		return nil
	}
	ret := make([]proto.Value, 0, len(indexes))
	for _, idx := range indexes {
		ret = append(ret, bp.Args[idx])
	}
	return ret
}
