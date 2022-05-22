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

package resultx

import (
	"github.com/arana-db/arana/pkg/proto"
)

var (
	_ proto.Result = (*noopResult)(nil)
	_ proto.Result = (*halfResult)(nil)
	_ proto.Result = (*fullResult)(nil)
	_ proto.Result = (*dsOnlyResult)(nil)
)

type option struct {
	ds           proto.Dataset
	id, affected uint64
}

type Option func(*option)

func WithLastInsertID(id uint64) Option {
	return func(o *option) {
		o.id = id
	}
}

func WithRowsAffected(n uint64) Option {
	return func(o *option) {
		o.affected = n
	}
}

func WithDataset(d proto.Dataset) Option {
	return func(o *option) {
		o.ds = d
	}
}

func New(options ...Option) proto.Result {
	var o option
	for _, it := range options {
		it(&o)
	}

	if o.ds == nil {
		if o.id == 0 && o.affected == 0 {
			return noopResult{}
		}
		return halfResult{o.id, o.affected}
	}

	if o.id == 0 && o.affected == 0 {
		return dsOnlyResult{ds: o.ds}
	}

	return fullResult{
		ds:       o.ds,
		id:       o.id,
		affected: o.affected,
	}
}

type noopResult struct {
}

func (n noopResult) Dataset() (proto.Dataset, error) {
	return nil, nil
}

func (n noopResult) LastInsertId() (uint64, error) {
	return 0, nil
}

func (n noopResult) RowsAffected() (uint64, error) {
	return 0, nil
}

type halfResult [2]uint64 // [lastInsertId,rowsAffected]

func (h halfResult) Dataset() (proto.Dataset, error) {
	return nil, nil
}

func (h halfResult) LastInsertId() (uint64, error) {
	return h[0], nil
}

func (h halfResult) RowsAffected() (uint64, error) {
	return h[1], nil
}

type fullResult struct {
	ds       proto.Dataset
	id       uint64
	affected uint64
}

func (f fullResult) Dataset() (proto.Dataset, error) {
	return f.ds, nil
}

func (f fullResult) LastInsertId() (uint64, error) {
	return f.id, nil
}

func (f fullResult) RowsAffected() (uint64, error) {
	return f.affected, nil
}

type dsOnlyResult struct {
	ds proto.Dataset
}

func (d dsOnlyResult) Dataset() (proto.Dataset, error) {
	return d.ds, nil
}

func (d dsOnlyResult) LastInsertId() (uint64, error) {
	return 0, nil
}

func (d dsOnlyResult) RowsAffected() (uint64, error) {
	return 0, nil
}

func Drain(result proto.Result) {
	if d, _ := result.Dataset(); d != nil {
		defer func() {
			_ = d.Close()
		}()
	}
	return
}
