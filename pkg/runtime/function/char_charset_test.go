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

package function

import (
	"context"
	"fmt"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

func TestFuncCastCharset(t *testing.T) {
	fn := proto.MustGetFunc(FuncCastCharset)
	assert.Equal(t, 2, fn.NumInput())

	type tt struct {
		inFirst  string
		inSecond string
		want     string
	}
	for _, v := range []tt{
		{"你好，世界", "utf8", "你好，世界"},
		{"これは漢字です。", "utf16le", "S0\x8c0o0\"oW[g0Y0\x020"},
		{"これは漢字です。", "utf16be", "0S0\x8c0oo\"[W0g0Y0\x02"},
		{"Résumé", "latin1", "R\xe9sum\xe9"},
		{"Gdańsk", "latin2", "Gda\xf1sk"},
		{"Kağan", "latin5", "Ka\xf0an"},
		{"Ââ Čč Đđ Ŋŋ Õõ Šš Žž Åå Ää", "latin6", "\xc2\xe2 \xc8\xe8 \xa9\xb9 \xaf\xbf \xd5\xf5 \xaa\xba \xac\xbc \xc5\xe5 \xc4\xe4"},
		{"latviešu", "latin7", "latvie\xf0u"},
		{"ελληνικά", "greek", "\xe5\xeb\xeb\xe7\xed\xe9\xea\xdc"},
		{"สำหรับ", "hebrew", "\xca\xd3\xcb\xc3\u047a"},
		{"Hello, world", "ascii", "Hello, world"},
		{"Résumé", "macintosh", "R\x8esum\x8e"},
		{"Gdańsk", "cp1250", "Gda\xf1sk"},
		{"русский", "cp1251", "\xf0\xf3\xf1\xf1\xea\xe8\xe9"},
		{"Résumé", "cp1252", "R\xe9sum\xe9"},
		{"ελληνικά", "cp1253", "\xe5\xeb\xeb\xe7\xed\xe9\xea\xdc"},
		{"Kağan", "cp1254", "Ka\xf0an"},
		{"עִבְרִית", "cp1255", "\xf2\xc4\xe1\xc0\xf8\xc4\xe9\xfa"},
		{"العربية", "cp1256", "\xc7\xe1\xda\xd1\xc8\xed\xc9"},
		{"latviešu", "cp1257", "latvie\xf0u"},
		{"Việt", "cp1258", "Vi\xea\xf2t"},
		{"สำหรับ", "cp874", "\xca\xd3\xcb\xc3\u047a"},
		{"русский", "koi8r", "\xd2\xd5\xd3\xd3\xcb\xc9\xca"},
		{"українська", "koi8u", "\xd5\xcb\xd2\xc1\xa7\xce\xd3\xd8\xcb\xc1"},
		{"Hello 常用國字標準字體表", "big5", "Hello \xb1`\xa5\u03b0\xea\xa6r\xbc\u0437\u01e6r\xc5\xe9\xaa\xed"},
		{"Hello 常用國字標準字體表", "gbk", "Hello \xb3\xa3\xd3\xc3\x87\xf8\xd7\xd6\x98\xcb\x9c\xca\xd7\xd6\xf3\x77\xb1\xed"},
		{"Hello 常用國字標準字體表", "gb18030", "Hello \xb3\xa3\xd3\xc3\x87\xf8\xd7\xd6\x98\xcb\x9c\xca\xd7\xd6\xf3\x77\xb1\xed"},
		{"עִבְרִית", "gb18030", "\x81\x30\xfb\x30\x81\x30\xf6\x34\x81\x30\xf9\x33\x81\x30\xf6\x30\x81\x30\xfb\x36\x81\x30\xf6\x34\x81\x30\xfa\x31\x81\x30\xfb\x38"},
		{"㧯", "gb18030", "\x82\x31\x89\x38"},
		{"これは漢字です。", "sjis", "\x82\xb1\x82\xea\x82\xcd\x8a\xbf\x8e\x9a\x82\xc5\x82\xb7\x81B"},
		{"Hello, 世界!", "sjis", "Hello, \x90\xa2\x8aE!"},
		{"ｲｳｴｵｶ", "sjis", "\xb2\xb3\xb4\xb5\xb6"},
		{"これは漢字です。", "ujis", "\xa4\xb3\xa4\xec\xa4\u03f4\xc1\xbb\xfa\xa4\u01e4\xb9\xa1\xa3"},
		{"다음과 같은 조건을 따라야 합니다: 저작자표시", "euckr", "\xb4\xd9\xc0\xbd\xb0\xfa \xb0\xb0\xc0\xba \xc1\xb6\xb0\xc7\xc0\xbb \xb5\xfb\xb6\xf3\xbe\xdf \xc7մϴ\xd9: \xc0\xfa\xc0\xdb\xc0\xdaǥ\xbd\xc3"},
	} {
		t.Run(v.want, func(t *testing.T) {
			first := proto.NewValueString(v.inSecond)
			second := proto.NewValueString(v.inFirst)
			out, err := fn.Apply(context.Background(), proto.ToValuer(first), proto.ToValuer(second))
			assert.NoError(t, err)
			assert.Equal(t, v.want, fmt.Sprint(out))
		})
	}
}
