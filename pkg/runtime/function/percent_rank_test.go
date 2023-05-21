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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestPercentRankDist(t *testing.T) {
	fn := proto.MustGetFunc(FuncPercentRank)
	type tt struct {
		inputs []proto.Value
		want   string
	}
	for _, v := range []tt{
		{
			[]proto.Value{
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(4),
				proto.NewValueInt64(4),
				proto.NewValueInt64(5),
			},
			"0",
		},
		{
			[]proto.Value{
				proto.NewValueInt64(2),
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(4),
				proto.NewValueInt64(4),
				proto.NewValueInt64(5),
			},
			"0.25",
		},
		{
			[]proto.Value{
				proto.NewValueInt64(3),
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(4),
				proto.NewValueInt64(4),
				proto.NewValueInt64(5),
			},
			"0.375",
		},
		{
			[]proto.Value{
				proto.NewValueInt64(4),
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(4),
				proto.NewValueInt64(4),
				proto.NewValueInt64(5),
			},
			"0.75",
		},
		{
			[]proto.Value{
				proto.NewValueInt64(5),
				proto.NewValueInt64(1),
				proto.NewValueInt64(1),
				proto.NewValueInt64(2),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(3),
				proto.NewValueInt64(4),
				proto.NewValueInt64(4),
				proto.NewValueInt64(5),
			},
			"1",
		},
	} {
		t.Run(v.want, func(t *testing.T) {
			var inputs []proto.Valuer
			for i := range v.inputs {
				inputs = append(inputs, proto.ToValuer(v.inputs[i]))
			}
			out, err := fn.Apply(context.Background(), inputs...)
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
