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
	"io"
	"regexp"
	"sync"
)

import (
	"github.com/pkg/errors"
)

var (
	_regexpTable     *regexp.Regexp
	_regexpTableOnce sync.Once
)

func getTableRegexp() *regexp.Regexp {
	_regexpTableOnce.Do(func() {
		_regexpTable = regexp.MustCompile(`([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`)
	})
	return _regexpTable
}

func ParseTable(input string) (db, tbl string, err error) {
	mat := getTableRegexp().FindStringSubmatch(input)
	if len(mat) < 1 {
		err = errors.Errorf("invalid table name: %s", input)
		return
	}
	db = mat[1]
	tbl = mat[2]
	return
}

func TryClose(i interface{}) error {
	if c, ok := i.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

func ReverseSlice[T any](input []T) {
	if len(input) < 2 {
		return
	}
	for i, j := 0, len(input)-1; i < j; {
		input[i], input[j] = input[j], input[i]
		i++
		j--
	}
}

// CartesianProduct compute cartesian product.
func CartesianProduct[T any](inputs [][]T) [][]T {
	var (
		res [][]T
		rec []T
	)
	cartesianProductHelper[T](inputs, &res, &rec, 0)
	return res
}

func cartesianProductHelper[T any](input [][]T, res *[][]T, rec *[]T, index int) {
	if index < len(input) {
		for _, v := range input[index] {
			*rec = append(*rec, v)
			cartesianProductHelper(input, res, rec, index+1)
			*rec = (*rec)[:index]
		}
		return
	}
	tmp := make([]T, len(input))
	copy(tmp, *rec)
	*res = append(*res, tmp)
	*rec = (*rec)[:index]
}
