package misc_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/arana-db/arana/pkg/util/misc"
)

func TestParseTable(t *testing.T) {
	tests := []struct {
		input string
		db    string
		tbl   string
		err   error
	}{
		{"dbname.tablename", "dbname", "tablename", nil},
		{"invalid", "", "", errors.New("invalid table name: invalid")},
		{"", "", "", errors.New("invalid table name: ")},
	}

	for _, tt := range tests {
		db, tbl, err := misc.ParseTable(tt.input)
		if db != tt.db || tbl != tt.tbl || (err != nil && err.Error() != tt.err.Error()) {
			t.Errorf("ParseTable(%s) = (%s, %s, %v), want (%s, %s, %v)", tt.input, db, tbl, err, tt.db, tt.tbl, tt.err)
		}
	}
}

func TestTryClose(t *testing.T) {
	var buf bytes.Buffer
	err := misc.TryClose(&buf)
	if err != nil {
		t.Errorf("TryClose() failed on a valid io.Closer: %v", err)
	}

	err = misc.TryClose(123) // an integer isn't an io.Closer
	if err != nil {
		t.Errorf("TryClose() failed on an invalid io.Closer: %v", err)
	}
}

func TestReverseSlice(t *testing.T) {
	tests := []struct {
		input    []int
		expected []int
	}{
		{[]int{1, 2, 3}, []int{3, 2, 1}},
		{[]int{}, []int{}},
		{[]int{1}, []int{1}},
	}

	for _, tt := range tests {
		misc.ReverseSlice(tt.input)
		for i, val := range tt.input {
			if val != tt.expected[i] {
				t.Errorf("ReverseSlice(%v) = %v, want %v", tt.input, tt.input, tt.expected)
				break
			}
		}
	}
}

func TestCartesianProduct(t *testing.T) {
	input := [][]int{{1, 2}, {3, 4}}
	expected := [][]int{{1, 3}, {1, 4}, {2, 3}, {2, 4}}
	result := misc.CartesianProduct(input)
	if len(result) != len(expected) {
		t.Fatalf("CartesianProduct(%v) has %d results, want %d", input, len(result), len(expected))
	}
	for i, slice := range result {
		for j, val := range slice {
			if val != expected[i][j] {
				t.Errorf("CartesianProduct(%v) = %v, want %v", input, result, expected)
				break
			}
		}
	}
}
