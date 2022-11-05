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
	_ proto.Result = (*emptyResult)(nil) // contains nothing
	_ proto.Result = (*slimResult)(nil)  // only contains rows-affected and last-insert-id, design for exec
	_ proto.Result = (*dsResult)(nil)    // only contains dataset, design for query
	_ proto.Result = (*fullResult)(nil)  // contains all
)

type option struct {
	ds           proto.Dataset
	id, affected uint64
}

// Option represents the option to create a result.
type Option func(*option)

// WithLastInsertID specify the last-insert-id for the result to be created.
func WithLastInsertID(id uint64) Option {
	return func(o *option) {
		o.id = id
	}
}

// WithRowsAffected specify the rows-affected for the result to be created.
func WithRowsAffected(n uint64) Option {
	return func(o *option) {
		o.affected = n
	}
}

// WithDataset specify the dataset for the result to be created.
func WithDataset(d proto.Dataset) Option {
	return func(o *option) {
		o.ds = d
	}
}

// New creates a result from some options.
func New(options ...Option) proto.Result {
	var o option
	for _, it := range options {
		it(&o)
	}

	// When execute EXEC, no need to specify dataset.
	if o.ds == nil {
		if o.id == 0 && o.affected == 0 {
			return emptyResult{}
		}
		return slimResult{o.id, o.affected}
	}

	// When execute QUERY, only dataset is required.
	if o.id == 0 && o.affected == 0 {
		return dsResult{ds: o.ds}
	}

	// should never happen
	return fullResult{
		ds:       o.ds,
		id:       o.id,
		affected: o.affected,
	}
}

type emptyResult struct{}

func (n emptyResult) Dataset() (proto.Dataset, error) {
	return nil, nil
}

func (n emptyResult) LastInsertId() (uint64, error) {
	return 0, nil
}

func (n emptyResult) RowsAffected() (uint64, error) {
	return 0, nil
}

type slimResult [2]uint64 // [lastInsertId,rowsAffected]

func (h slimResult) Dataset() (proto.Dataset, error) {
	return nil, nil
}

func (h slimResult) LastInsertId() (uint64, error) {
	return h[0], nil
}

func (h slimResult) RowsAffected() (uint64, error) {
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

type dsResult struct {
	ds proto.Dataset
}

func (d dsResult) Dataset() (proto.Dataset, error) {
	return d.ds, nil
}

func (d dsResult) LastInsertId() (uint64, error) {
	return 0, nil
}

func (d dsResult) RowsAffected() (uint64, error) {
	return 0, nil
}

func Drain(result proto.Result) {
	d, _ := result.Dataset()
	if d == nil {
		return
	}
	_ = d.Close()
}
