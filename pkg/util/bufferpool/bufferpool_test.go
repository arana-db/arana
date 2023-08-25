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

package bufferpool

import (
	"bytes"
	"testing"
)

func TestGet(t *testing.T) {
	buf := Get()
	if buf == nil {
		t.Error("Expected non-nil buffer")
	}
}

func TestPut(t *testing.T) {
	buf := new(bytes.Buffer)
	Put(buf)
	// Ensure buffer is returned to the pool
	// and can be borrowed again
	buf2 := Get()
	if buf != buf2 {
		t.Error("Expected borrowed buffer to be the same as the put buffer")
	}
}

func TestPutNilBuffer(t *testing.T) {
	Put(nil)
	// Ensure that putting a nil buffer does not cause any error
}

func TestPutHugeBuffer(t *testing.T) {
	hugeBuf := bytes.NewBuffer(make([]byte, 0, 2*1024*1024))
	Put(hugeBuf)
	// Ensure that putting a huge buffer does not cause any error
}

func TestPutResetBuffer(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.WriteString("Hello, World!")
	Put(buf)
	// Ensure buffer is reset before it is put back to the pool
	buf2 := Get()
	if buf2.Len() != 0 {
		t.Error("Expected reset buffer to have zero length")
	}
}
