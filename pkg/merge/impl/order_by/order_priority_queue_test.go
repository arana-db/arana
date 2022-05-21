package order_by

import (
	"reflect"
	"testing"
)

func TestNewPriorityQueue(t *testing.T) {
	tests := []struct {
		name string
		want OrderPriorityQueue
	}{
		{name: "order_priority_queue", want: OrderPriorityQueue{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewOrderPriorityQueue(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewOrderPriorityQueue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderPriorityQueue_Len(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{name: "order_priority_queue_len", fields: fields{
			[]*OrderByValue{{}, {}},
		}, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			if got := pq.Len(); got != tt.want {
				t.Errorf("Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderPriorityQueue_Less(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			if got := pq.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderPriorityQueue_Peek(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			if got := pq.Peek(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Peek() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderPriorityQueue_Pop(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			if got := pq.Pop(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Pop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderPriorityQueue_Push(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	type args struct {
		x interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "order_priority_queue_push", fields: fields{
			[]*OrderByValue{{}, {}},
		}, args: args{x: NewOrderByValue(nil, nil)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			pq.Push(tt.args.x)
		})
	}
}

func TestOrderPriorityQueue_Swap(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			pq.Swap(tt.args.i, tt.args.j)
		})
	}
}

func TestOrderPriorityQueue_update(t *testing.T) {
	type fields struct {
		orderByValues []*OrderByValue
	}
	tests := []struct {
		name   string
		fields fields
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := &OrderPriorityQueue{
				orderByValues: tt.fields.orderByValues,
			}
			pq.update()
		})
	}
}
