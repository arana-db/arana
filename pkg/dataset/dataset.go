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
	"github.com/arana-db/arana/pkg/proto"
)

// PeekableDataset represents a peekable dataset.
type PeekableDataset interface {
	proto.Dataset
	// Peek peeks the next row, but will not consume it.
	Peek() (proto.Row, error)
}

type RandomAccessDataset interface {
	PeekableDataset
	// Len returns the length of sub-datasets.
	Len() int
	// PeekN peeks the next row with specified index.
	PeekN(index int) (proto.Row, error)
	// SetNextN force sets the next index of row.
	SetNextN(index int) error
}
