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
	"sync"
)

import (
	"github.com/pkg/errors"

	uatomic "go.uber.org/atomic"

	"golang.org/x/sync/errgroup"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	_ PeekableDataset     = (*peekableDataset)(nil)
	_ RandomAccessDataset = (*parallelDataset)(nil)
)

type peekableDataset struct {
	proto.Dataset
	mu   sync.Mutex
	next proto.Row
	err  error
}

func (pe *peekableDataset) Next() (proto.Row, error) {
	var (
		next proto.Row
		err  error
	)

	pe.mu.Lock()
	defer pe.mu.Unlock()

	if next, err, pe.next, pe.err = pe.next, pe.err, nil, nil; next != nil || err != nil {
		return next, err
	}

	return pe.Dataset.Next()
}

func (pe *peekableDataset) Peek() (proto.Row, error) {
	var (
		next proto.Row
		err  error
	)

	pe.mu.Lock()
	defer pe.mu.Unlock()

	if next, err = pe.next, pe.err; next != nil || err != nil {
		return next, err
	}

	pe.next, pe.err = pe.Dataset.Next()

	return pe.next, pe.err
}

type parallelDataset struct {
	mu         sync.RWMutex
	fields     []proto.Field
	generators []GenerateFunc
	streams    []*peekableDataset
	seq        uatomic.Uint32
}

func (pa *parallelDataset) getStream(i int) (*peekableDataset, error) {
	pa.mu.RLock()
	if stream := pa.streams[i]; stream != nil {
		pa.mu.RUnlock()
		return stream, nil
	}

	pa.mu.RUnlock()

	pa.mu.Lock()
	defer pa.mu.Unlock()

	if stream := pa.streams[i]; stream != nil {
		return stream, nil
	}

	d, err := pa.generators[i]()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	pa.streams[i] = &peekableDataset{Dataset: d}
	pa.generators[i] = nil
	return pa.streams[i], nil
}

func (pa *parallelDataset) Peek() (proto.Row, error) {
	i := pa.seq.Load() % uint32(pa.Len())
	s, err := pa.getStream(int(i))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return s.Peek()
}

func (pa *parallelDataset) PeekN(index int) (proto.Row, error) {
	if index < 0 || index >= pa.Len() {
		return nil, errors.Errorf("index out of range: index=%d, length=%d", index, pa.Len())
	}
	s, err := pa.getStream(index)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return s.Peek()
}

func (pa *parallelDataset) Close() error {
	var g errgroup.Group
	for i := 0; i < len(pa.streams); i++ {
		i := i
		g.Go(func() (err error) {
			if pa.streams[i] != nil {
				err = pa.streams[i].Close()
			}

			if err != nil {
				log.Errorf("failed to close dataset#%d: %v", i, err)
			}
			return
		})
	}
	return g.Wait()
}

func (pa *parallelDataset) Fields() ([]proto.Field, error) {
	return pa.fields, nil
}

func (pa *parallelDataset) Next() (proto.Row, error) {
	var (
		s    *peekableDataset
		next proto.Row
		err  error
	)
	for j := 0; j < pa.Len(); j++ {
		i := (pa.seq.Inc() - 1) % uint32(pa.Len())

		if s, err = pa.getStream(int(i)); err != nil {
			return nil, errors.WithStack(err)
		}

		next, err = s.Next()
		if err == nil {
			break
		}
		if errors.Is(err, io.EOF) {
			err = io.EOF
			continue
		}

		return nil, errors.WithStack(err)
	}

	return next, err
}

func (pa *parallelDataset) Len() int {
	return len(pa.streams)
}

func (pa *parallelDataset) SetNextN(index int) error {
	if index < 0 || index >= pa.Len() {
		return errors.Errorf("index out of range: index=%d, length=%d", index, pa.Len())
	}
	pa.seq.Store(uint32(index))
	return nil
}

// Parallel creates a thread-safe dataset, which can be random-accessed in parallel.
func Parallel(first GenerateFunc, others ...GenerateFunc) (RandomAccessDataset, error) {
	current, err := first()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create parallel datasets")
	}

	fields, err := current.Fields()
	if err != nil {
		defer func() {
			_ = current.Close()
		}()
		return nil, errors.WithStack(err)
	}

	generators := make([]GenerateFunc, len(others)+1)
	for i := 0; i < len(others); i++ {
		if others[i] == nil {
			return nil, errors.Errorf("nil dataset detected, index is %d", i+1)
		}
		generators[i+1] = others[i]
	}

	streams := make([]*peekableDataset, len(others)+1)
	streams[0] = &peekableDataset{Dataset: current}

	return &parallelDataset{
		fields:     fields,
		generators: generators,
		streams:    streams,
	}, nil
}

type parallelBuilder struct {
	genFuns []GenerateFunc
}

func NewParallelBuilder() parallelBuilder {
	return parallelBuilder{}
}

func (pb *parallelBuilder) Add(genFunc GenerateFunc) {
	pb.genFuns = append(pb.genFuns, genFunc)
}

func (pb *parallelBuilder) Build() (RandomAccessDataset, error) {
	if len(pb.genFuns) == 0 {
		return nil, errors.New("failed to create parallel datasets")
	}
	if len(pb.genFuns) == 1 {
		return Parallel(pb.genFuns[0], nil)
	}
	return Parallel(pb.genFuns[0], pb.genFuns[1:]...)
}

// Peekable converts a dataset to a peekable one.
func Peekable(origin proto.Dataset) PeekableDataset {
	return &peekableDataset{
		Dataset: origin,
	}
}
