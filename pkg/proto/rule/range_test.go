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

package rule

import (
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestStepper_Date_After(t *testing.T) {
	hourSt := Stepper{
		N: 2,
		U: Uhour,
	}
	daySt := Stepper{
		N: 2,
		U: Uday,
	}
	testTime := time.Date(2021, 1, 17, 17, 45, 04, 0, time.UTC)
	hour, err := hourSt.After(testTime)
	assert.NoError(t, err)
	assert.Equal(t, 19, hour.(time.Time).Hour())
	day, err := daySt.After(testTime)
	assert.NoError(t, err)
	assert.Equal(t, 19, day.(time.Time).Day())
}

func TestStepper_After(t *testing.T) {
	st := Stepper{
		N: 2,
		U: Unum,
	}
	val, err := st.After(2)
	assert.NoError(t, err)
	assert.Equal(t, 4, val)
}

func TestStepper_Before(t *testing.T) {
	st := Stepper{
		N: 1,
		U: Unum,
	}
	val, err := st.Before(2)
	assert.NoError(t, err)
	assert.Equal(t, 1, val)
}

func TestStepper_Ascend(t *testing.T) {
	st := Stepper{
		N: 1,
		U: Unum,
	}

	rng, err := st.Ascend(100, 3)
	assert.NoError(t, err)

	var vals []int
	for rng.HasNext() {
		vals = append(vals, rng.Next().(int))
	}
	assert.Equal(t, []int{100, 101, 102}, vals)
}

func TestStepper_Descend(t *testing.T) {
	st := Stepper{
		N: 1,
		U: Unum,
	}

	rng, err := st.Descend(100, 3)
	assert.NoError(t, err)

	var vals []int
	for rng.HasNext() {
		vals = append(vals, rng.Next().(int))
	}
	assert.Equal(t, []int{100, 99, 98}, vals)
}
