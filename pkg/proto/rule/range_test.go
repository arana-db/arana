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
	t.Log(hourSt.String())
	daySt := Stepper{
		N: 2,
		U: Uday,
	}
	t.Log(daySt.String())
	testTime := time.Date(2021, 1, 17, 17, 45, 04, 0, time.UTC)
	hour, err := hourSt.After(testTime)
	assert.NoError(t, err)
	assert.Equal(t, 19, hour.(time.Time).Hour())
	day, err := daySt.After(testTime)
	assert.NoError(t, err)
	assert.Equal(t, 19, day.(time.Time).Day())
}

func TestStepper_After(t *testing.T) {
	type tt struct {
		st     Stepper
		offset interface{}
		expect interface{}
	}

	date := parseDate("2022-01-01 00:00:00")

	for _, it := range []tt{
		{Stepper{N: 2, U: Unum}, 2, 4},
		{Stepper{N: 2, U: Unum}, int32(2), int32(4)},
		{Stepper{N: 2, U: Unum}, int64(2), int64(4)},
		{Stepper{N: 1, U: Uhour}, date, parseDate("2022-01-01 01:00:00")},
		{Stepper{N: 1, U: Uday}, date, parseDate("2022-01-02 00:00:00")},
		{Stepper{N: 1, U: Uweek}, date, parseDate("2022-01-08 00:00:00")},
	} {
		t.Run(it.st.String(), func(t *testing.T) {
			val, err := it.st.After(it.offset)
			assert.NoError(t, err)
			assert.Equal(t, it.expect, val)
		})
	}
}

func TestStepper_Before(t *testing.T) {
	type tt struct {
		st     Stepper
		offset interface{}
		expect interface{}
	}

	date := parseDate("2022-01-01 00:00:00")

	for _, it := range []tt{
		{Stepper{N: 2, U: Unum}, 4, 2},
		{Stepper{N: 2, U: Unum}, int32(4), int32(2)},
		{Stepper{N: 2, U: Unum}, int64(4), int64(2)},
		{Stepper{N: 1, U: Uhour}, date, parseDate("2021-12-31 23:00:00")},
		{Stepper{N: 1, U: Uday}, date, parseDate("2021-12-31 00:00:00")},
		{Stepper{N: 1, U: Uweek}, date, parseDate("2021-12-25 00:00:00")},
	} {
		t.Run(it.st.String(), func(t *testing.T) {
			val, err := it.st.Before(it.offset)
			assert.NoError(t, err)
			assert.Equal(t, it.expect, val)
		})
	}
}

func TestStepper_Ascend(t *testing.T) {
	t.Run("WithNil", func(t *testing.T) {
		st := Stepper{N: 1, U: Unum}
		_, err := st.Ascend(nil, 1)
		assert.Error(t, err)
	})

	type tt struct {
		st     Stepper
		offset interface{}
		n      int
		expect []interface{}
	}

	date := parseDate("2022-01-01 00:00:00")

	for _, it := range []tt{
		{Stepper{N: 1, U: Unum}, 100, 3, []interface{}{100, 101, 102}},
		{Stepper{N: 1, U: Unum}, int32(100), 3, []interface{}{int32(100), int32(101), int32(102)}},
		{Stepper{N: 1, U: Unum}, int64(100), 3, []interface{}{int64(100), int64(101), int64(102)}},
		{Stepper{N: 1, U: Uhour}, date, 3, []interface{}{parseDate("2022-01-01 00:00:00"), parseDate("2022-01-01 01:00:00"), parseDate("2022-01-01 02:00:00")}},
		{Stepper{N: 1, U: Uday}, date, 3, []interface{}{parseDate("2022-01-01 00:00:00"), parseDate("2022-01-02 00:00:00"), parseDate("2022-01-03 00:00:00")}},
	} {
		t.Run(it.st.String(), func(t *testing.T) {
			rng, err := it.st.Ascend(it.offset, it.n)
			assert.NoError(t, err)

			var vals []interface{}
			for rng.HasNext() {
				vals = append(vals, rng.Next())
			}
			assert.Equal(t, it.expect, vals)
		})
	}
}

func TestStepper_Descend(t *testing.T) {
	t.Run("WithNil", func(t *testing.T) {
		st := Stepper{N: 1, U: Unum}
		_, err := st.Descend(nil, 1)
		assert.Error(t, err)
	})

	type tt struct {
		st     Stepper
		offset interface{}
		n      int
		expect []interface{}
	}

	date := parseDate("2022-01-01 00:00:00")

	for _, it := range []tt{
		{Stepper{N: 1, U: Unum}, 100, 3, []interface{}{100, 99, 98}},
		{Stepper{N: 1, U: Unum}, int32(100), 3, []interface{}{int32(100), int32(99), int32(98)}},
		{Stepper{N: 1, U: Unum}, int64(100), 3, []interface{}{int64(100), int64(99), int64(98)}},
		{Stepper{N: 1, U: Uhour}, date, 3, []interface{}{parseDate("2022-01-01 00:00:00"), parseDate("2021-12-31 23:00:00"), parseDate("2021-12-31 22:00:00")}},
		{Stepper{N: 1, U: Uday}, date, 3, []interface{}{parseDate("2022-01-01 00:00:00"), parseDate("2021-12-31 00:00:00"), parseDate("2021-12-30 00:00:00")}},
	} {
		t.Run(it.st.String(), func(t *testing.T) {
			rng, err := it.st.Descend(it.offset, it.n)
			assert.NoError(t, err)

			var vals []interface{}
			for rng.HasNext() {
				vals = append(vals, rng.Next())
			}
			assert.Equal(t, it.expect, vals)
		})
	}
}

func TestStepUnit_String(t *testing.T) {
	type tt struct {
		u      StepUnit
		isTime bool
	}

	for _, it := range []tt{
		{Uyear, true},
		{Umonth, true},
		{Uday, true},
		{Uweek, true},
		{Uhour, true},
		{Unum, false},
		{Ustr, false},
	} {
		assert.NotEqual(t, "UNKNOWN", it.u.String())
		assert.Equal(t, it.isTime, it.u.IsTime())
	}
	assert.Equal(t, "UNKNOWN", StepUnit(0x7F).String())
}

func parseDate(s string) time.Time {
	ret, _ := time.Parse("2006-01-02 15:04:05", s)
	return ret
}
