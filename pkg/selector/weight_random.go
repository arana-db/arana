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

package selector

import (
	"math/rand"
	"time"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

// DefaultWeight .
var DefaultWeight = 10

type weightRandom struct {
	weightValues   []int
	weightAreaEnds []int
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (w weightRandom) GetDataSourceNo() int {
	areaSize := len(w.weightAreaEnds)
	randSeed := rand.Intn(w.weightAreaEnds[areaSize-1])
	for i := 0; i < areaSize; i++ {
		if randSeed < w.weightAreaEnds[i] {
			return i
		}
	}
	return 0
}

// setWeight set weights
func (w *weightRandom) setWeight(weights []int) {
	w.weightValues = weights
	w.genAreaEnds()
}

func (w *weightRandom) genAreaEnds() {
	if len(w.weightValues) == 0 {
		return
	}
	weightSize := len(w.weightValues)
	w.weightAreaEnds = make([]int, weightSize)
	sum := 0
	for i := 0; i < weightSize; i++ {
		sum += w.weightValues[i]
		w.weightAreaEnds[i] = sum
	}
	if sum == 0 {
		log.Infof("generate %v from %v", w.weightValues, w.weightAreaEnds)
	}
}

// NewWeightRandomSelector new weight random selector
func NewWeightRandomSelector(weights []int) Selector {
	w := &weightRandom{}
	w.setWeight(weights)
	return w
}
