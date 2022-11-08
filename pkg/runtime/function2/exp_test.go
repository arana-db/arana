package function2

import (
	"context"
	"fmt"
	"testing"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestExp(t *testing.T) {
	fn := proto.MustGetFunc(FuncExp)
	assert.Equal(t, 1, fn.NumInput())

	type tt struct {
		in  proto.Value
		out string
	}

	mustDecimal := func(s string) *gxbig.Decimal {
		d, _ := gxbig.NewDecFromString(s)
		return d
	}

	for _, it := range []tt{
		{0, "1"},
		{int64(-123), "0"},
		{-3.14, "0.043282797901965896"},
		{float32(2.78), "16.119020948027543"},
		{mustDecimal("-5.1234"), "0.005955738919461573"},
		{"-618", "0"},
		{"-11.11", "0.0000149619536854"},
		{"foobar", "1.0"},
	} {
		t.Run(it.out, func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(it.in))
			assert.NoError(t, err)
			assert.Equal(t, it.out, fmt.Sprint(out))
		})
	}
}
