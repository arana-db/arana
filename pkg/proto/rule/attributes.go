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

package rule

import (
	"encoding/binary"
)

type attributes struct {
	inner map[byte][]byte
}

func (a *attributes) attribute(key byte) ([]byte, bool) {
	if a == nil {
		return nil, false
	}
	b, ok := a.inner[key]
	return b, ok
}

func (a *attributes) setAttribute(key byte, value []byte) {
	if a.inner == nil {
		a.inner = make(map[byte][]byte)
	}
	a.inner[key] = value
}

func (a *attributes) attributeBool(key byte) (value bool, ok bool) {
	var exist []byte

	if exist, ok = a.attribute(key); !ok {
		return
	}

	if len(exist) > 0 && exist[0] != 0x00 {
		value = true
	}

	return
}

func (a *attributes) setAttributeBool(key byte, value bool) {
	if value {
		a.setAttribute(key, []byte{0x01})
	} else {
		a.setAttribute(key, []byte{0x00})
	}
}

func (a *attributes) setAttributeUint32(key byte, value uint32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, value)
	a.setAttribute(key, b)
}

func (a *attributes) attributeUint32(key byte) (uint32, bool) {
	exist, ok := a.attribute(key)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint32(exist), true
}

func (a *attributes) setAttributeUint64(key byte, value uint64) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, value)
	a.setAttribute(key, b)
}

func (a *attributes) attributeUint64(key byte) (uint64, bool) {
	exist, ok := a.attribute(key)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(exist), true
}
