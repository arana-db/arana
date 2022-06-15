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
	"sync"
)

var _bufferPool sync.Pool

// Get borrows a Buffer from pool.
func Get() *bytes.Buffer {
	if exist, ok := _bufferPool.Get().(*bytes.Buffer); ok {
		return exist
	}
	return new(bytes.Buffer)
}

// Put returns a Buffer to pool.
func Put(b *bytes.Buffer) {
	if b == nil {
		return
	}
	const maxCap = 1024 * 1024
	// drop huge buff directly, if cap is over 1MB
	if b.Cap() > maxCap {
		return
	}
	b.Reset()
	_bufferPool.Put(b)
}
