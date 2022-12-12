package math

import (
	"fmt"
	"testing"
)

func TestCodeAndDecode(t *testing.T) {
	const (
		a    = int64(692)
		b    = int64(3)
		size = int64(16)
	)

	res, err := Code(a, size, b)
	if err != nil {
		t.Fatal(err)
	}

	aa, bb := Decode(res, size)
	if aa != a {
		t.Errorf(fmt.Sprintf("expect %d, got %d", a, aa))
	}
	if bb != b {
		t.Errorf(fmt.Sprintf("expect %d, got %d", b, bb))
	}
}
