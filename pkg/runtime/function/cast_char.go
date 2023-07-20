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
	"bytes"
	"context"
	"io"
	"strings"
	"unicode/utf8"
)

import (
	"github.com/pkg/errors"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/runes"
)

// FuncCastChar is  https://dev.mysql.com/doc/refman/5.6/en/cast-functions.html#function_cast
const FuncCastChar = "CAST_CHAR"

var _ proto.Func = (*castcharFunc)(nil)

func init() {
	proto.RegisterFunc(FuncCastChar, castcharFunc{})
}

type castcharFunc struct{}

func (a castcharFunc) Apply(ctx context.Context, inputs ...proto.Valuer) (proto.Value, error) {
	// expr
	val1, err := inputs[0].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if len(inputs) < 3 {
		return val1, nil
	}

	val2, err := inputs[1].Value(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	d2, _ := val2.Decimal()

	// N
	num := d2.IntPart()

	// charset_info
	val3, err := inputs[2].Value(ctx)
	if err != nil {
		return nil, err
	}

	s, err := a.getResult(runes.ConvertToRune(val1), num, val3.String())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return proto.NewValueString(s), nil
}

func (a castcharFunc) NumInput() int {
	return 3
}

func (a castcharFunc) getResult(runes []rune, num int64, charEncode string) (string, error) {
	var srcString string

	// N
	if num > int64(len(runes)) {
		srcString = string(runes)
	} else if num >= 0 {
		srcString = string(runes[:num])
	} else {
		srcString = string(runes)
	}

	// charset_info
	if !utf8.ValidString(srcString) || strings.EqualFold(charEncode, "") {
		// source string only support utf8
		return srcString, nil
	}
	charInfo := strings.Split(charEncode, " ")
	if len(charInfo) >= 3 && strings.EqualFold(charInfo[0], "CHARACTER") && strings.EqualFold(charInfo[1], "SET") {
		if strings.EqualFold(charInfo[2], "gbk") {
			// CHARACTER SET gbk
			srcEncode := simplifiedchinese.GBK.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] gbk encode error")
			}
		} else if strings.EqualFold(charInfo[2], "gb18030") {
			// CHARACTER SET gb18030
			srcEncode := simplifiedchinese.GB18030.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] gb18030 encode error")
			}
		} else if strings.EqualFold(charInfo[2], "latin2") {
			// CHARACTER SET latin2
			srcEncode := charmap.ISO8859_2.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] latin2 encode error")
			}
		} else if strings.EqualFold(charInfo[2], "latin5") {
			// CHARACTER SET latin5
			srcEncode := charmap.ISO8859_9.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] latin5 encode error")
			}
		} else if strings.EqualFold(charInfo[2], "greek") {
			// CHARACTER SET greek
			srcEncode := charmap.ISO8859_7.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] greek encode error")
			}
		} else if strings.EqualFold(charInfo[2], "hebrew") {
			// CHARACTER SET hebrew
			srcEncode := charmap.ISO8859_8.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] hebrew encode error")
			}
		} else if strings.EqualFold(charInfo[2], "latin7") {
			// CHARACTER SET latin7
			srcEncode := charmap.ISO8859_13.NewEncoder()
			dstString, err := srcEncode.String(srcString)
			if err == nil {
				return dstString, nil
			} else {
				return "", errors.New("CHAR[(N)][charset_info] latin7 encode error")
			}
		} else {
			return "", errors.New("CHAR[(N)][charset_info] Variable charset_info is not supported")
		}
	} else if len(charInfo) >= 1 && strings.EqualFold(charInfo[0], "ASCII") {
		// ASCII: CHARACTER SET latin1
		srcEncode := charmap.ISO8859_1.NewEncoder()
		dstString, err := srcEncode.String(srcString)
		if err == nil {
			return dstString, nil
		} else {
			return "", errors.New("CHAR[(N)][charset_info] latin1 encode error")
		}
	} else if len(charInfo) >= 1 && strings.EqualFold(charInfo[0], "UNICODE") {
		// UNICODE: CHARACTER SET ucs2
		srcReader := bytes.NewReader([]byte(srcString))
		// UTF-16 bigendian, no-bom
		trans := transform.NewReader(srcReader,
			unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder())
		dstString, err := io.ReadAll(trans)
		if err == nil {
			return string(dstString), nil
		} else {
			return "", errors.New("CHAR[(N)][charset_info] ucs2 encode error")
		}
	} else {
		return "", errors.New("CHAR[(N)][charset_info] Variable charset_info is invalid")
	}
}
