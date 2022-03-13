//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package aggregater

import (
	gxbig "github.com/dubbogo/gost/math/big"
)

type MinAggregater struct {
	min  *gxbig.Decimal
	init bool
}

func (s *MinAggregater) Aggregate(values []interface{}) {
	if len(values) == 0 {
		return
	}

	val, err := parseDecimal2(values[0])
	if err != nil {
		panic(err)
	}

	if !s.init {
		s.min = val
		s.init = true
	} else if s.min.Compare(val) > 0 {
		s.min = val
	}
}

func (s *MinAggregater) GetResult() (*gxbig.Decimal, bool) {
	if s.init {
		return s.min, true
	}
	return nil, false
}
