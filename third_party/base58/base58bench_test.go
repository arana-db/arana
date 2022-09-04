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

// Copyright (c) 2013-2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package base58_test

import (
	"bytes"
	"testing"
)

import (
	"github.com/arana-db/arana/third_party/base58"
)

var (
	raw5k       = bytes.Repeat([]byte{0xff}, 5000)
	raw100k     = bytes.Repeat([]byte{0xff}, 100*1000)
	encoded5k   = base58.Encode(raw5k)
	encoded100k = base58.Encode(raw100k)
)

func BenchmarkBase58Encode_5K(b *testing.B) {
	b.SetBytes(int64(len(raw5k)))
	for i := 0; i < b.N; i++ {
		base58.Encode(raw5k)
	}
}

func BenchmarkBase58Encode_100K(b *testing.B) {
	b.SetBytes(int64(len(raw100k)))
	for i := 0; i < b.N; i++ {
		base58.Encode(raw100k)
	}
}

func BenchmarkBase58Decode_5K(b *testing.B) {
	b.SetBytes(int64(len(encoded5k)))
	for i := 0; i < b.N; i++ {
		base58.Decode(encoded5k)
	}
}

func BenchmarkBase58Decode_100K(b *testing.B) {
	b.SetBytes(int64(len(encoded100k)))
	for i := 0; i < b.N; i++ {
		base58.Decode(encoded100k)
	}
}
