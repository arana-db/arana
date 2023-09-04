package runes

import (
	"reflect"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestConvertToRune(t *testing.T) {
	tests := []struct {
		input  interface{}
		output []rune
	}{
		{123, []rune("123")},
		{"hello", []rune("hello")},
		{true, []rune("true")},
		{nil, []rune("<nil>")},
		{struct{ name string }{name: "John"}, []rune("{John}")},
	}

	for _, test := range tests {
		result := ConvertToRune(test.input)
		assert.True(t, reflect.DeepEqual(result, test.output))
	}
}
