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

// Copyright 2020 Gin Core Team. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package charsetconv

import (
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestCharsetConv(t *testing.T) {
	deocdeFn := Decode
	encodeFn := Encode
	type tt struct {
		EncStrFirst  string // utf8 encoding string
		EncStrSecond string // other encoding string
		charset      string // charset
	}
	ttCase := []tt{
		{"你好，世界", "你好，世界", "utf8"},
		{"これは漢字です。", "S0\x8c0o0\"oW[g0Y0\x020", "utf16le"},
		{"これは漢字です。", "0S0\x8c0oo\"[W0g0Y0\x02", "utf16be"},
		{"Résumé", "R\xe9sum\xe9", "latin1"},
		{"Gdańsk", "Gda\xf1sk", "latin2"},
		{"Kağan", "Ka\xf0an", "latin5"},
		{"Ââ Čč Đđ Ŋŋ Õõ Šš Žž Åå Ää", "\xc2\xe2 \xc8\xe8 \xa9\xb9 \xaf\xbf \xd5\xf5 \xaa\xba \xac\xbc \xc5\xe5 \xc4\xe4", "latin6"},
		{"latviešu", "latvie\xf0u", "latin7"},
		{"ελληνικά", "\xe5\xeb\xeb\xe7\xed\xe9\xea\xdc", "greek"},
		{"สำหรับ", "\xca\xd3\xcb\xc3\u047a", "hebrew"},
		{"Hello, world", "Hello, world", "ascii"},
		{"Résumé", "R\x8esum\x8e", "macintosh"},
		{"Gdańsk", "Gda\xf1sk", "cp1250"},
		{"русский", "\xf0\xf3\xf1\xf1\xea\xe8\xe9", "cp1251"},
		{"Résumé", "R\xe9sum\xe9", "cp1252"},
		{"ελληνικά", "\xe5\xeb\xeb\xe7\xed\xe9\xea\xdc", "cp1253"},
		{"Kağan", "Ka\xf0an", "cp1254"},
		{"עִבְרִית", "\xf2\xc4\xe1\xc0\xf8\xc4\xe9\xfa", "cp1255"},
		{"العربية", "\xc7\xe1\xda\xd1\xc8\xed\xc9", "cp1256"},
		{"latviešu", "latvie\xf0u", "cp1257"},
		{"Việt", "Vi\xea\xf2t", "cp1258"},
		{"สำหรับ", "\xca\xd3\xcb\xc3\u047a", "cp874"},
		{"русский", "\xd2\xd5\xd3\xd3\xcb\xc9\xca", "koi8r"},
		{"українська", "\xd5\xcb\xd2\xc1\xa7\xce\xd3\xd8\xcb\xc1", "koi8u"},
		{"Hello 常用國字標準字體表", "Hello \xb1`\xa5\u03b0\xea\xa6r\xbc\u0437\u01e6r\xc5\xe9\xaa\xed", "big5"},
		{"Hello 常用國字標準字體表", "Hello \xb3\xa3\xd3\xc3\x87\xf8\xd7\xd6\x98\xcb\x9c\xca\xd7\xd6\xf3\x77\xb1\xed", "gbk"},
		{"Hello 常用國字標準字體表", "Hello \xb3\xa3\xd3\xc3\x87\xf8\xd7\xd6\x98\xcb\x9c\xca\xd7\xd6\xf3\x77\xb1\xed", "gb18030"},
		{"עִבְרִית", "\x81\x30\xfb\x30\x81\x30\xf6\x34\x81\x30\xf9\x33\x81\x30\xf6\x30\x81\x30\xfb\x36\x81\x30\xf6\x34\x81\x30\xfa\x31\x81\x30\xfb\x38", "gb18030"},
		{"㧯", "\x82\x31\x89\x38", "gb18030"},
		{"これは漢字です。", "\x82\xb1\x82\xea\x82\xcd\x8a\xbf\x8e\x9a\x82\xc5\x82\xb7\x81B", "sjis"},
		{"Hello, 世界!", "Hello, \x90\xa2\x8aE!", "sjis"},
		{"ｲｳｴｵｶ", "\xb2\xb3\xb4\xb5\xb6", "sjis"},
		{"これは漢字です。", "\xa4\xb3\xa4\xec\xa4\u03f4\xc1\xbb\xfa\xa4\u01e4\xb9\xa1\xa3", "ujis"},
		{"다음과 같은 조건을 따라야 합니다: 저작자표시", "\xb4\xd9\xc0\xbd\xb0\xfa \xb0\xb0\xc0\xba \xc1\xb6\xb0\xc7\xc0\xbb \xb5\xfb\xb6\xf3\xbe\xdf \xc7մϴ\xd9: \xc0\xfa\xc0\xdb\xc0\xdaǥ\xbd\xc3", "euckr"},
	}
	// test decode
	for _, v := range ttCase {
		t.Run(v.EncStrFirst, func(t *testing.T) {
			c, err := GetCharset(v.charset)
			assert.NoError(t, err)
			out, err := deocdeFn(v.EncStrSecond, c)
			assert.NoError(t, err)
			assert.Equal(t, v.EncStrFirst, fmt.Sprint(out))
		})
	}
	// test encode
	for _, v := range ttCase {
		t.Run(v.EncStrSecond, func(t *testing.T) {
			c, err := GetCharset(v.charset)
			assert.NoError(t, err)
			out, err := encodeFn(v.EncStrFirst, c)
			assert.NoError(t, err)
			assert.Equal(t, v.EncStrSecond, fmt.Sprint(out))
		})
	}
}
