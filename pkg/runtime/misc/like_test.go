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

package misc

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLiker(t *testing.T) {
	liker := NewLiker("abc%")
	assert.Equal(t, true, liker.Like("abc*"))
	assert.Equal(t, true, liker.Like("abc?"))
	assert.Equal(t, true, liker.Like("abc_"))
	assert.Equal(t, true, liker.Like("abc%"))
	assert.Equal(t, false, liker.Like("zmo"))

	liker = NewLiker("abc_")
	assert.Equal(t, true, liker.Like("abc*"))
	assert.Equal(t, true, liker.Like("abc?"))
	assert.Equal(t, true, liker.Like("abc_"))
	assert.Equal(t, true, liker.Like("abc%"))
	assert.Equal(t, false, liker.Like("abc"))

	liker = NewLiker("abc?")
	assert.Equal(t, true, liker.Like("abc?"))
	assert.Equal(t, false, liker.Like("abc*"))
	assert.Equal(t, false, liker.Like("abc_"))
	assert.Equal(t, false, liker.Like("abc%"))

	liker = NewLiker("*?abc_")
	assert.Equal(t, true, liker.Like("*?abc."))
	assert.Equal(t, false, liker.Like("**abc."))
	assert.Equal(t, false, liker.Like("??abc."))
	assert.Equal(t, false, liker.Like("?*abc."))
	assert.Equal(t, false, liker.Like("??abc."))

	liker = NewLiker("[CB]at")
	assert.Equal(t, true, liker.Like("[CB]at"))
	assert.Equal(t, false, liker.Like("Cat"))
	assert.Equal(t, false, liker.Like("cat"))

	liker = NewLiker("[a-z]at")
	assert.Equal(t, true, liker.Like("[a-z]at"))
	assert.Equal(t, false, liker.Like("cat"))
	assert.Equal(t, false, liker.Like("Cat"))

	liker = NewLiker("[!C]at")
	assert.Equal(t, true, liker.Like("[!C]at"))
	assert.Equal(t, false, liker.Like("Bat"))
	assert.Equal(t, false, liker.Like("Cat"))

	liker = NewLiker("Letter[!3-5]")
	assert.Equal(t, true, liker.Like("Letter[!3-5]"))
	assert.Equal(t, false, liker.Like("Letter1"))
	assert.Equal(t, false, liker.Like("Letter5"))

	liker = NewLiker("/home/\\\\*")
	assert.Equal(t, false, liker.Like("/home/\\*"))

	liker = NewLiker("\\%")
	assert.Equal(t, true, liker.Like("\\\\"))

	liker = NewLiker("/hom%")
	assert.Equal(t, true, liker.Like("/home/hello/world"))
}
