package function

import (
	"context"
	"fmt"
	"unicode/utf8"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

// FuncCharLength is  https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_character-length
const FuncCharLength = "CHAR_LENGTH"

var _ proto.Func = (*charlengthFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCharLength, charlengthFunc{})
}

type charlengthFunc struct{}

func (a charlengthFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	val, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	result := utf8.RuneCountInString(fmt.Sprint(val))
	return int64(result), nil
}

func (a charlengthFunc) NumInput() int {
	return 1
}
