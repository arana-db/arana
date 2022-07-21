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
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/testdata"
)

func TestFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRange := testdata.NewMockRange(ctrl)
	predicate := func(interface{}) bool {
		return true
	}
	type args struct {
		src       rule.Range
		predicate func(interface{}) bool
	}
	tests := []struct {
		name string
		args args
		want rule.Range
	}{
		{
			"TestFilter",
			args{mockRange, predicate},
			&filterRange{inner: mockRange, filter: predicate},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Filter(tt.args.src, tt.args.predicate), "Filter(%v, %v)", tt.args.src, tt.args.predicate)
		})
	}
}

func TestMultiple(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	slice := make([]interface{}, 0)
	for i := 0; i < 3; i++ {
		mockRange := testdata.NewMockRange(ctrl)
		slice = append(slice, mockRange)
	}
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name string
		args args
		want rule.Range
	}{
		{"TestMultiple", args{slice}, &sliceRange{0, slice}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Multiple(tt.args.values...), "Multiple(%v)", tt.args.values...)
		})
	}
}

func TestSingle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRange := testdata.NewMockRange(ctrl)
	type args struct {
		value interface{}
	}
	tests := []struct {
		name string
		args args
		want rule.Range
	}{
		{"TestSingle", args{mockRange}, &singleRange{0, mockRange}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Single(tt.args.value), "Single(%v)", tt.args.value)
		})
	}
}

func Test_filterRange_HasNext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	filter := func(interface{}) bool { return true }
	mockRange := testdata.NewMockRange(ctrl)
	mockRange.EXPECT().HasNext().Return(true)
	mockRange.EXPECT().Next().Return(filter)
	type fields struct {
		inner  rule.Range
		filter func(next interface{}) bool
		next   interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"HasNext", fields{mockRange, filter, nil}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &filterRange{
				inner:  tt.fields.inner,
				filter: tt.fields.filter,
				next:   tt.fields.next,
			}
			assert.Equalf(t, tt.want, f.HasNext(), "HasNext()")
		})
	}
}

func Test_filterRange_Next(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRange := testdata.NewMockRange(ctrl)
	filter := func(interface{}) bool { return true }
	type fields struct {
		inner  rule.Range
		filter func(next interface{}) bool
		next   interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"Next", fields{mockRange, filter, "filter_range_next"}, "filter_range_next"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &filterRange{
				inner:  tt.fields.inner,
				filter: tt.fields.filter,
				next:   tt.fields.next,
			}
			assert.Equalf(t, tt.want, f.Next(), "Next()")
		})
	}
}

func Test_singleRange_HasNext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRange := testdata.NewMockRange(ctrl)
	type fields struct {
		n     uint8
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"HasNext", fields{0, mockRange}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &singleRange{
				n:     tt.fields.n,
				value: tt.fields.value,
			}
			assert.Equalf(t, tt.want, s.HasNext(), "HasNext()")
		})
	}
}

func Test_singleRange_Next(t *testing.T) {
	type fields struct {
		n     uint8
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"Next", fields{0, "single_range_next"}, "single_range_next"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &singleRange{
				n:     tt.fields.n,
				value: tt.fields.value,
			}
			assert.Equalf(t, tt.want, s.Next(), "Next()")
		})
	}
}

func Test_sliceRange_HasNext(t *testing.T) {
	value := make([]interface{}, 0)
	value = append(value, "a")
	value = append(value, "b")
	type fields struct {
		cur    int
		values []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"HasNext_1", fields{0, value}, true},
		{"HasNext_2", fields{2, value}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sliceRange{
				cur:    tt.fields.cur,
				values: tt.fields.values,
			}
			assert.Equalf(t, tt.want, s.HasNext(), "HasNext()")
		})
	}
}

func Test_sliceRange_Next(t *testing.T) {
	value := make([]interface{}, 0)
	value = append(value, "a")
	value = append(value, "b")
	type fields struct {
		cur    int
		values []interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"Next_1", fields{0, value}, "a"},
		{"Next_2", fields{1, value}, "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &sliceRange{
				cur:    tt.fields.cur,
				values: tt.fields.values,
			}
			assert.Equalf(t, tt.want, s.Next(), "Next()")
		})
	}
}
