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

package mysql

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestReadBool(t *testing.T) {
	table := []struct {
		in           string
		value, valid bool
	}{
		{in: "0", value: false, valid: true},
		{in: "false", value: false, valid: true},
		{in: "FALSE", value: false, valid: true},
		{in: "False", value: false, valid: true},
		{in: "1", value: true, valid: true},
		{in: "true", value: true, valid: true},
		{in: "TRUE", value: true, valid: true},
		{in: "True", value: true, valid: true},
		{in: "FAlse", value: false, valid: false},
	}
	for i := 0; i < len(table); i++ {
		value, valid := readBool(table[i].in)
		assert.Equal(t, table[i].value, value)
		assert.Equal(t, table[i].valid, valid)
	}
}

func TestBToi(t *testing.T) {
	table := []struct {
		in   byte
		want int
		err  error
	}{
		{in: '0', want: 0, err: nil},
		{in: '1', want: 1, err: nil},
		{in: '4', want: 4, err: nil},
		{in: '5', want: 5, err: nil},
		{in: '9', want: 9, err: nil},
		{in: 'a', want: 0, err: errors.New("not [0-9]")},
	}
	for i := 0; i < len(table); i++ {
		res, err := bToi(table[i].in)
		if err != nil {
			assert.EqualError(t, err, err.Error())
		}
		assert.Equal(t, table[i].want, res)
	}
}

func TestParseByteNanoSec(t *testing.T) {
	table := []struct {
		in   []byte
		want int
	}{
		{in: []byte{'1'}, want: 100000000},
		{in: []byte{'1', '2', '3'}, want: 123000000},
		{in: []byte{'1', '2', '3', '4', '5', '6'}, want: 123456000},
		{in: []byte{'1', '2', '3', '4', '5', '6', '7'}, want: 123456000},
	}
	for i := 0; i < len(table); i++ {
		res, err := parseByteNanoSec(table[i].in)
		assert.NoError(t, err)
		assert.Equal(t, table[i].want, res)
	}
}

func TestParseDateTime(t *testing.T) {
	table := []struct {
		in   []byte
		want time.Time
		err  error
	}{
		{in: []byte("2022-1-1"), want: time.Time{}, err: fmt.Errorf("invalid time bytes: %s", "2022-1-1")},
		{in: []byte("2022-01-01"), want: time.Date(2022, 1, 1, 0, 0, 0, 0, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10"), want: time.Date(2022, 1, 1, 10, 10, 10, 0, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.1"), want: time.Date(2022, 1, 1, 10, 10, 10, 100000000, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.11"), want: time.Date(2022, 1, 1, 10, 10, 10, 110000000, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.111"), want: time.Date(2022, 1, 1, 10, 10, 10, 111000000, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.1111"), want: time.Date(2022, 1, 1, 10, 10, 10, 111100000, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.11111"), want: time.Date(2022, 1, 1, 10, 10, 10, 111110000, time.Local), err: nil},
		{in: []byte("2022-01-01 10:10:10.111111"), want: time.Date(2022, 1, 1, 10, 10, 10, 111111000, time.Local), err: nil},
	}
	for i := 0; i < len(table); i++ {
		res, err := parseDateTime(table[i].in, time.Local)
		if err != nil {
			assert.EqualError(t, err, table[i].err.Error())
		}
		assert.Equal(t, table[i].want, res)
	}
}

func TestParseByteYear(t *testing.T) {
	table := []struct {
		in   []byte
		want int
	}{
		{in: []byte("0101"), want: 101},
		{in: []byte("2022"), want: 2022},
	}
	for i := 0; i < len(table); i++ {
		res, err := parseByteYear(table[i].in)
		assert.NoError(t, err)
		assert.Equal(t, table[i].want, res)
	}
}

func TestParseByte2Digits(t *testing.T) {
	table := []struct {
		inFirst, inSecond byte
		want              int
	}{
		{inFirst: '0', inSecond: '1', want: 1},
		{inFirst: '0', inSecond: '3', want: 3},
		{inFirst: '1', inSecond: '0', want: 10},
		{inFirst: '1', inSecond: '1', want: 11},
		{inFirst: '1', inSecond: '2', want: 12},
	}
	for i := 0; i < len(table); i++ {
		res, err := parseByte2Digits(table[i].inFirst, table[i].inSecond)
		assert.NoError(t, err)
		assert.Equal(t, table[i].want, res)
	}
}

func TestParseBinaryDateTime(t *testing.T) {
	table := []struct {
		num  uint64
		data []byte
		want driver.Value
	}{
		{num: 0, data: []byte(""), want: time.Time{}},
		{num: 4, data: []byte{byte(230), byte(7), byte(1), byte(1)}, want: time.Date(2022, 01, 01, 0, 0, 0, 0, time.Local)},
		{num: 7, data: []byte{byte(230), byte(7), byte(1), byte(1), byte(1), byte(1), byte(1)}, want: time.Date(2022, 01, 01, 1, 1, 1, 0, time.Local)},
		{num: 11, data: []byte{byte(230), byte(7), byte(1), byte(1), byte(1), byte(1), byte(1), byte(87), byte(4), byte(0), byte(0)}, want: time.Date(2022, 01, 01, 1, 1, 1, 1111000, time.Local)},
	}
	for i := 0; i < len(table); i++ {
		res, err := parseBinaryDateTime(table[i].num, table[i].data, time.Local)
		assert.NoError(t, err)
		assert.Equal(t, table[i].want, res)
	}
}
