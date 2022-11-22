package function

import (
	"context"
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestCharLength(t *testing.T) {
	fn := proto.MustGetFunc(FuncCharLength)
	assert.Equal(t, 1, fn.NumInput())
	type tt struct {
		inFirst proto.Value
		want    int64
	}
	for _, v := range []tt{
		{"Hello世界", 7},
		{"Hello", 5},
		{"世界", 2},
		{"你好世界！", 5},
	} {
		t.Run(fmt.Sprint(v.inFirst), func(t *testing.T) {
			out, err := fn.Apply(context.Background(), proto.ToValuer(v.inFirst))
			assert.NoError(t, err)
			assert.Equal(t, v.want, out)
		})
	}

}
