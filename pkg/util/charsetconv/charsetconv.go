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
	"io/ioutil"
	"strings"
)

import (
	"github.com/pkg/errors"

	"golang.org/x/net/html/charset"

	"golang.org/x/text/transform"
)

type Charset string

// See the support of mysql 8.0 for character sets
var charsetAlias = map[string]Charset{
	// Unicode
	"utf8":    "UTF-8",
	"utf16":   "UTF-16",
	"utf16le": "UTF-16LE",
	"utf16be": "UTF-16BE",

	// Chinese
	"gbk":     "GBK",
	"gb18030": "GB18030",
	"gb2312":  "GB2312",
	"big5":    "Big5",

	// ASCII
	"ascii": "ASCII",

	// Windows
	"cp1250": "windows-1250",
	"cp1251": "windows-1251",
	"cp1252": "windows-1252",
	"cp1253": "windows-1253",
	"cp1254": "windows-1254",
	"cp1255": "windows-1255",
	"cp1256": "windows-1256",
	"cp1257": "windows-1257",
	"cp1258": "windows-1258",
	"cp850":  "windows-850",
	"cp852":  "windows-852",
	"cp866":  "windows-866",
	"cp874":  "windows-874",
	"cp932":  "windows-932",

	// ISO
	"latin1": "cp1252",
	"latin2": "ISO-8859-2",
	"latin5": "ISO-8859-9",
	"greek":  "ISO-8859-7",
	"latin6": "ISO-8859-10",
	"hebrew": "ISO-8859-11",
	"latin7": "ISO-8859-13",

	// other
	"sjis":      "SJIS",
	"koi8r":     "KOI8-R",
	"koi8u":     "KOI8-U",
	"ucs2":      "UCS-2",
	"ujis":      "EUC-JP",
	"euckr":     "EUC-KR",
	"macintosh": "macintosh",
}

func GetCharset(charset string) (Charset, error) {
	c, ok := charsetAlias[string(charset)]
	if !ok {
		return "", errors.New(fmt.Sprintf("Unknown character set: '%v'", charset))
	}
	return c, nil
}

func transformString(t transform.Transformer, s string) (string, error) {
	r := transform.NewReader(strings.NewReader(s), t)
	b, err := ioutil.ReadAll(r)
	return string(b), err
}

// Decode from other encoding type string to utf8
func Decode(otherEncodeStr string, encodeType Charset) (string, error) {
	e, _ := charset.Lookup(string(encodeType))
	if e == nil {
		return "", errors.New(fmt.Sprintf("%v: not found", encodeType))
	}
	decodeStr, err := transformString(e.NewDecoder(), string(otherEncodeStr))
	if err != nil {
		return "", err
	}
	return decodeStr, nil
}

// Encode from utf8 string to other encoding type
func Encode(utf8EncodeStr string, encodeType Charset) (string, error) {
	e, _ := charset.Lookup(string(encodeType))
	if e == nil {
		return "", errors.New(fmt.Sprintf("%v: not found", encodeType))
	}
	encodeStr, err := transformString(e.NewEncoder(), string(utf8EncodeStr))
	if err != nil {
		return "", err
	}
	return encodeStr, nil
}
