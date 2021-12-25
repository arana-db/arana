// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package misc

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestUnescape(t *testing.T) {
	assert.Equal(t, `abc\abc`, Unescape(`abc\\abc`))
	assert.Equal(t, "abc\nabc", Unescape(`abc\nabc`))
	assert.Equal(t, "\\abc\\\\abc\n\t\rabc\v", Unescape(`\\abc\\\\abc\n\t\rabc\v`))
}

func TestEscape(t *testing.T) {
	assert.Equal(t, `hello\nworld!`, Escape("hello\nworld!", 0))
	assert.Equal(t, `{\"age\":18}`, Escape(`{"age":18}`, EscapeDoubleQuote))
	assert.Equal(t, `like\%`, Escape(`like\%`, EscapeLike))
}
