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
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
)

var _ proto.Dataset = (*fuseDataset)(nil)

type GenerateFunc func() (proto.Dataset, error)

type fuseDataset struct {
	fields     []proto.Field
	current    proto.Dataset
	generators []GenerateFunc
}

func (fu *fuseDataset) Close() error {
	if fu.current == nil {
		return nil
	}
	if err := fu.current.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (fu *fuseDataset) Fields() ([]proto.Field, error) {
	return fu.fields, nil
}

func (fu *fuseDataset) Next() (proto.Row, error) {
	if fu.current == nil {
		return nil, io.EOF
	}

	var (
		next proto.Row
		err  error
	)

	if next, err = fu.current.Next(); errors.Is(err, io.EOF) {
		if err = fu.nextDataset(); err != nil {
			return nil, errors.WithStack(err)
		}
		return fu.Next()
	}

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return next, nil
}

func (fu *fuseDataset) nextDataset() error {
	var err error
	if err = fu.current.Close(); err != nil {
		return errors.Wrap(err, "failed to close previous fused dataset")
	}
	fu.current = nil

	if len(fu.generators) < 1 {
		return io.EOF
	}

	gen := fu.generators[0]
	fu.generators[0] = nil
	fu.generators = fu.generators[1:]

	if fu.current, err = gen(); err != nil {
		return errors.Wrap(err, "failed to close previous fused dataset")
	}

	return nil
}

func Fuse(first GenerateFunc, others ...GenerateFunc) (proto.Dataset, error) {
	current, err := first()
	if err != nil {
		return nil, errors.Wrap(err, "failed to fuse datasets")
	}

	fields, err := current.Fields()
	if err != nil {
		defer func() {
			_ = current.Close()
		}()
		return nil, errors.WithStack(err)
	}

	return &fuseDataset{
		fields:     fields,
		current:    current,
		generators: others,
	}, nil
}
