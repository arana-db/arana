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
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

var _ proto.Dataset = (*TransformDataset)(nil)

type (
	FieldsFunc    func([]proto.Field) []proto.Field
	TransformFunc func(proto.Row) (proto.Row, error)
)

type TransformDataset struct {
	proto.Dataset
	FieldsGetter FieldsFunc
	Transform    TransformFunc

	actualFieldsOnce    sync.Once
	actualFields        []proto.Field
	actualFieldsFailure error
}

func (td *TransformDataset) Fields() ([]proto.Field, error) {
	td.actualFieldsOnce.Do(func() {
		origin, err := td.Dataset.Fields()
		if err != nil {
			td.actualFieldsFailure = err
			return
		}
		if td.FieldsGetter == nil {
			td.actualFields = origin
			return
		}

		td.actualFields = td.FieldsGetter(origin)
	})

	return td.actualFields, td.actualFieldsFailure
}

func (td *TransformDataset) Next() (proto.Row, error) {
	if td.Transform == nil {
		return td.Dataset.Next()
	}

	var (
		row proto.Row
		err error
	)

	if row, err = td.Dataset.Next(); err != nil {
		return nil, err
	}

	if row, err = td.Transform(row); err != nil {
		return nil, errors.Wrap(err, "failed to transform dataset")
	}

	return row, nil
}
