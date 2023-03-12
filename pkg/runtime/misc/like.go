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

package misc

import (
	"path/filepath"
	"strings"
)

var replacer = strings.NewReplacer(
	"%", "*",
	"_", "?",
	"*", "\u0000",
	"?", "\u0001",
	"[", "\u0002",
	"]", "\u0003",
	"\\", "\u0004",
	"/", "\u0005",
)

type Liker interface {
	Like(s string) bool
}

type liker struct {
	pattern string
}

func NewLiker(pattern string) Liker {
	return &liker{pattern: pattern}
}

func (l *liker) Like(s string) bool {
	p := l.pattern
	p = replacer.Replace(p)
	s = replacer.Replace(s)
	flag, _ := filepath.Match(p, s)
	return flag
}
