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

package aggregater

import (
	gxbig "github.com/dubbogo/gost/math/big"
)

type AddAggregater struct {
	count *gxbig.Decimal
}

func (s *AddAggregater) Aggregate(values []interface{}) {
	if len(values) == 0 {
		return
	}

	val1, err := parseDecimal2(values[0])
	if err != nil {
		panic(err)
	}
	if s.count == nil {
		s.count = &gxbig.Decimal{}
	}
	gxbig.DecimalAdd(s.count, val1, s.count)
}

func (s *AddAggregater) GetResult() (*gxbig.Decimal, bool) {
	return s.count, true
}
