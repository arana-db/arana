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

package rows

import (
	"testing"
	"time"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestBinaryValueWriter(t *testing.T) {
	var w BinaryValueWriter

	n, err := w.WriteString("foo")
	assert.NoError(t, err)
	assert.Equal(t, int64(4), n)
	assert.Equal(t, []byte{0x03, 0x66, 0x6f, 0x6f}, w.Bytes())

	w.Reset()
	n, err = w.WriteUint64(1)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), n)
	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, w.Bytes())

	w.Reset()
	n, err = w.WriteUint32(1)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), n)
	assert.Equal(t, []byte{0x01, 0x00, 0x00, 0x00}, w.Bytes())

	w.Reset()
	n, err = w.WriteUint16(1)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.Equal(t, []byte{0x01, 0x00}, w.Bytes())

	w.Reset()
	n, err = w.WriteFloat64(10.2)
	assert.NoError(t, err)
	assert.Equal(t, int64(8), n)
	assert.Equal(t, []byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x24, 0x40}, w.Bytes())

	w.Reset()
	n, err = w.WriteFloat32(10.2)
	assert.NoError(t, err)
	assert.Equal(t, int64(4), n)
	assert.Equal(t, []byte{0x33, 0x33, 0x23, 0x41}, w.Bytes())

	// -120d 19:27:30.000 001
	w.Reset()
	n, err = w.WriteDuration(-(120*24*time.Hour + 19*time.Hour + 27*time.Minute + 30*time.Second + 1*time.Microsecond))
	assert.NoError(t, err)
	assert.Equal(t, int64(13), n)
	assert.Equal(t, []byte{0x0c, 0x01, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e, 0x01, 0x00, 0x00, 0x00}, w.Bytes())

	// -120d 19:27:30
	w.Reset()
	n, err = w.WriteDuration(-(120*24*time.Hour + 19*time.Hour + 27*time.Minute + 30*time.Second))
	assert.NoError(t, err)
	assert.Equal(t, int64(9), n)
	assert.Equal(t, []byte{0x08, 0x01, 0x78, 0x00, 0x00, 0x00, 0x13, 0x1b, 0x1e}, w.Bytes())

	// 0d 00:00:00
	w.Reset()
	n, err = w.WriteDuration(0)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, []byte{0x00}, w.Bytes())
}
