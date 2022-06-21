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

package dataset

import (
	"io"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

var _ proto.Dataset = (*VirtualDataset)(nil)

type VirtualDataset struct {
	Columns []proto.Field
	Rows    []proto.Row
}

func (cu *VirtualDataset) Close() error {
	return nil
}

func (cu *VirtualDataset) Fields() ([]proto.Field, error) {
	return cu.Columns, nil
}

func (cu *VirtualDataset) Next() (proto.Row, error) {
	if len(cu.Rows) < 1 {
		return nil, io.EOF
	}

	next := cu.Rows[0]

	cu.Rows[0] = nil
	cu.Rows = cu.Rows[1:]

	return next, nil
}
