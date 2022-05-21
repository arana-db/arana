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

package order_by

import (
	"reflect"
	"testing"
)

import (
	"github.com/arana-db/arana/pkg/merge"
)

func TestNewOrderByStreamMergeRows(t *testing.T) {
	type args struct {
		rows         []*merge.MergeRows
		orderByItems []OrderByItem
	}
	tests := []struct {
		name string
		args args
		want *OrderByStreamMergeRows
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewOrderByStreamMergeRows(tt.args.rows, tt.args.orderByItems); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewOrderByStreamMergeRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderByStreamMergeRows_buildCurrentRow(t *testing.T) {
	type fields struct {
		orderByItems []OrderByItem
		queue        OrderPriorityQueue
		rows         []*merge.MergeRows
		isFirstNext  bool
		currentRow   *merge.MergeRows
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := OrderByStreamMergeRows{
				orderByItems: tt.fields.orderByItems,
				queue:        tt.fields.queue,
				rows:         tt.fields.rows,
				isFirstNext:  tt.fields.isFirstNext,
				currentRow:   tt.fields.currentRow,
			}
			rows.buildCurrentRow()
		})
	}
}

func TestOrderByStreamMergeRows_next(t *testing.T) {
	type fields struct {
		orderByItems []OrderByItem
		queue        OrderPriorityQueue
		rows         []*merge.MergeRows
		isFirstNext  bool
		currentRow   *merge.MergeRows
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := OrderByStreamMergeRows{
				orderByItems: tt.fields.orderByItems,
				queue:        tt.fields.queue,
				rows:         tt.fields.rows,
				isFirstNext:  tt.fields.isFirstNext,
				currentRow:   tt.fields.currentRow,
			}
			if got := rows.next(); got != tt.want {
				t.Errorf("next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buildMergeRowsToQueue(t *testing.T) {
	type args struct {
		rows         []*merge.MergeRows
		orderByItems []OrderByItem
	}
	tests := []struct {
		name string
		args args
		want OrderPriorityQueue
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildMergeRowsToQueue(tt.args.rows, tt.args.orderByItems); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("buildMergeRowsToQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}
