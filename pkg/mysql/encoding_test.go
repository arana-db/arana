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

package mysql

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLenEncIntSize(t *testing.T) {
	assert.Equal(t, 1, lenEncIntSize(1))
	assert.Equal(t, 3, lenEncIntSize(1<<8))
	assert.Equal(t, 4, lenEncIntSize(1<<16))
	assert.Equal(t, 9, lenEncIntSize(1<<24))
}

func TestWriteLenEncInt(t *testing.T) {
	data := make([]byte, 9)
	writeLenEncInt(data, 0, 1)
	assert.Equal(t, uint8(0x1), data[0])
	num, _, _ := readLenEncInt(data, 0)
	assert.Equal(t, uint64(0x1), num)
	writeLenEncInt(data, 0, 1<<8)
	assert.Equal(t, uint8(0xfc), data[0])
	assert.Equal(t, uint8(0x00), data[1])
	assert.Equal(t, uint8(0x01), data[2])
	num, _, _ = readLenEncInt(data, 0)
	assert.Equal(t, uint64(0x1)<<8, num)
	writeLenEncInt(data, 0, 1<<16)
	assert.Equal(t, uint8(0xfd), data[0])
	assert.Equal(t, uint8(0x00), data[1])
	assert.Equal(t, uint8(0x00), data[2])
	assert.Equal(t, uint8(0x01), data[3])
	num, _, _ = readLenEncInt(data, 0)
	assert.Equal(t, uint64(0x1)<<16, num)
	writeLenEncInt(data, 0, 1<<24)
	assert.Equal(t, uint8(0xfe), data[0])
	assert.Equal(t, uint8(0x00), data[1])
	assert.Equal(t, uint8(0x00), data[2])
	assert.Equal(t, uint8(0x00), data[3])
	assert.Equal(t, uint8(0x01), data[4])
	assert.Equal(t, uint8(0x00), data[5])
	assert.Equal(t, uint8(0x00), data[6])
	assert.Equal(t, uint8(0x00), data[7])
	assert.Equal(t, uint8(0x00), data[8])
	num, _, _ = readLenEncInt(data, 0)
	assert.Equal(t, uint64(0x1)<<24, num)
}

func TestUint64(t *testing.T) {
	data := make([]byte, 8)
	writeUint64(data, 0, 1<<24)
	assert.Equal(t, uint8(0x00), data[0])
	assert.Equal(t, uint8(0x00), data[1])
	assert.Equal(t, uint8(0x00), data[2])
	assert.Equal(t, uint8(0x01), data[3])
	assert.Equal(t, uint8(0x00), data[4])
	assert.Equal(t, uint8(0x00), data[5])
	assert.Equal(t, uint8(0x00), data[6])
	assert.Equal(t, uint8(0x00), data[7])
	num, _, _ := readUint64(data, 0)
	assert.Equal(t, uint64(1<<24), num)
}

func TestEncString(t *testing.T) {
	str := "for test test test"
	assert.Equal(t, len(str)+1, lenEncStringSize(str))
	data := make([]byte, len(str)+1)
	writeLenEncString(data, 0, str)
	assert.Equal(t, uint8(len(str)), data[0])
	copyStr, _, _ := readBytesCopy(data, 1, len(str))
	assert.Equal(t, str, string(copyStr))
	read, _, _ := readLenEncString(data, 0)
	assert.Equal(t, str, read)
	pos, _ := skipLenEncString(data, 0)
	assert.Equal(t, len(str)+1, pos)
	copyStr, _, _ = readLenEncStringAsBytesCopy(data, 0)
	assert.Equal(t, str, string(copyStr))
}
