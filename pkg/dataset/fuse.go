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

var _ proto.Dataset = (*FuseableDataset)(nil)

type GenerateFunc func() (proto.Dataset, error)

type FuseableDataset struct {
	fields     []proto.Field
	current    proto.Dataset
	generators []GenerateFunc
}

func (fu *FuseableDataset) Close() error {
	if fu.current == nil {
		return nil
	}
	if err := fu.current.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (fu *FuseableDataset) Fields() ([]proto.Field, error) {
	return fu.fields, nil
}

func (fu *FuseableDataset) Next() (proto.Row, error) {
	if fu.current == nil {
		return nil, io.EOF
	}

	var (
		next proto.Row
		err  error
	)

	if next, err = fu.current.Next(); errors.Is(err, io.EOF) {
		if err = fu.nextDataset(); err != nil {
			return nil, err
		}
		return fu.Next()
	}

	if err != nil {
		return nil, err
	}

	return next, nil
}

func (fu *FuseableDataset) nextDataset() error {
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

func (fu *FuseableDataset) ToParallel() RandomAccessDataset {
	generators := make([]GenerateFunc, len(fu.generators)+1)
	for i := 0; i < len(fu.generators); i++ {
		generators[i+1] = fu.generators[i]
	}
	streams := make([]*peekableDataset, len(fu.generators)+1)
	streams[0] = &peekableDataset{Dataset: fu.current}
	result := &parallelDataset{
		fields:     fu.fields,
		generators: generators,
		streams:    streams,
	}
	return result
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

	return &FuseableDataset{
		fields:     fields,
		current:    current,
		generators: others,
	}, nil
}
