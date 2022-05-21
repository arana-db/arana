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
