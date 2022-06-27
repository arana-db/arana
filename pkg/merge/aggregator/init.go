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

package aggregator

import (
	"fmt"
)

import (
	"github.com/arana-db/arana/pkg/merge"
)

var aggregatorMap = make(map[string]func() merge.Aggregator)

func init() {
	aggregatorMap["MAX"] = func() merge.Aggregator { return &MaxAggregator{} }
	aggregatorMap["MIN"] = func() merge.Aggregator { return &MinAggregator{} }
	aggregatorMap["COUNT"] = func() merge.Aggregator { return &AddAggregator{} }
	aggregatorMap["SUM"] = func() merge.Aggregator { return &AddAggregator{} }
}

func GetAggFromName(name string) func() merge.Aggregator {
	if agg, ok := aggregatorMap[name]; ok {
		return agg
	}
	panic(fmt.Errorf("aggregator %s not support yet", name))
}
