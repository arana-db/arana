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
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Dataset = (*GroupDataset)(nil)

// Reducer represents the way to reduce rows.
type Reducer interface {
	// Reduce reduces next row.
	Reduce(next proto.Row) error
	// Row returns the result row.
	Row() proto.Row
}

type GroupDataset struct {
	proto.Dataset
	keys []string

	fieldFunc           FieldsFunc
	actualFieldsOnce    sync.Once
	actualFields        []proto.Field
	actualFieldsFailure error

	keyIndexes        []int
	keyIndexesOnce    sync.Once
	keyIndexesFailure error

	reducer func() Reducer

	buf proto.Row
	eof bool
}

func (gd *GroupDataset) Close() error {
	return gd.Dataset.Close()
}

func (gd *GroupDataset) Fields() ([]proto.Field, error) {
	gd.actualFieldsOnce.Do(func() {
		if gd.fieldFunc == nil {
			gd.actualFields, gd.actualFieldsFailure = gd.Dataset.Fields()
			return
		}

		defer func() {
			gd.fieldFunc = nil
		}()

		fields, err := gd.Dataset.Fields()
		if err != nil {
			gd.actualFieldsFailure = err
			return
		}
		gd.actualFields = gd.fieldFunc(fields)
	})

	return gd.actualFields, gd.actualFieldsFailure
}

func (gd *GroupDataset) Next() (proto.Row, error) {
	if gd.eof {
		return nil, io.EOF
	}

	indexes, err := gd.getKeyIndexes()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var (
		rowsChan = make(chan proto.Row, 1)
		errChan  = make(chan error, 1)
	)

	go func() {
		defer close(rowsChan)
		gd.consumeUntilDifferent(indexes, rowsChan, errChan)
	}()

	reducer := gd.reducer()

L:
	for {
		select {
		case next, ok := <-rowsChan:
			if !ok {
				break L
			}
			if err = reducer.Reduce(next); err != nil {
				break L
			}
		case err = <-errChan:
			break L
		}
	}

	if err != nil {
		return nil, err
	}

	return reducer.Row(), nil
}

func (gd *GroupDataset) consumeUntilDifferent(indexes []int, rowsChan chan<- proto.Row, errChan chan<- error) {
	var (
		next proto.Row
		err  error
	)

	for {
		next, err = gd.Dataset.Next()
		if errors.Is(err, io.EOF) {
			gd.eof = true

			if buf, ok := gd.popBuf(); ok {
				rowsChan <- buf
			}
			break
		}

		if err != nil {
			errChan <- err
			break
		}

		prev, ok := gd.popBuf()
		gd.buf = next
		if !ok {
			log.Debugf("begin next group: %s", gd.toDebugStr(next))
			continue
		}

		ok, err = gd.isSameGroup(indexes, prev, next)

		if err != nil {
			errChan <- err
			break
		}

		rowsChan <- prev

		if !ok {
			log.Debugf("begin next group: %s", gd.toDebugStr(next))
			break
		}
	}
}

func (gd *GroupDataset) toDebugStr(next proto.Row) string {
	var (
		display    []string
		fields, _  = gd.Dataset.Fields()
		indexes, _ = gd.getKeyIndexes()
		dest       = make([]proto.Value, len(fields))
	)

	_ = next.Scan(dest)
	for _, it := range indexes {
		display = append(display, fmt.Sprintf("%s:%v", fields[it].Name(), dest[it]))
	}

	return fmt.Sprintf("[%s]", strings.Join(display, ","))
}

func (gd *GroupDataset) isSameGroup(indexes []int, prev, next proto.Row) (bool, error) {
	var (
		fields, _ = gd.Dataset.Fields()
		err       error
	)

	// TODO: reduce scan times, maybe cache it.
	var (
		dest0 = make([]proto.Value, len(fields))
		dest1 = make([]proto.Value, len(fields))
	)
	if err = prev.Scan(dest0); err != nil {
		return false, errors.WithStack(err)
	}
	if err = next.Scan(dest1); err != nil {
		return false, errors.WithStack(err)
	}

	equal := true
	for _, index := range indexes {
		// TODO: how to compare equality more effectively?
		if !reflect.DeepEqual(dest0[index], dest1[index]) {
			equal = false
			break
		}
	}

	return equal, nil
}

func (gd *GroupDataset) popBuf() (ret proto.Row, ok bool) {
	if gd.buf != nil {
		ret, gd.buf, ok = gd.buf, nil, true
	}
	return
}

// getKeyIndexes computes and holds the indexes of group keys.
func (gd *GroupDataset) getKeyIndexes() ([]int, error) {
	gd.keyIndexesOnce.Do(func() {
		var (
			fields []proto.Field
			err    error
		)

		if fields, err = gd.Dataset.Fields(); err != nil {
			gd.keyIndexesFailure = err
			return
		}
		gd.keyIndexes = make([]int, 0, len(gd.keys))
		for _, key := range gd.keys {
			idx := -1
			for i := 0; i < len(fields); i++ {
				if fields[i].Name() == key {
					idx = i
					break
				}
			}
			if idx == -1 {
				gd.keyIndexesFailure = fmt.Errorf("cannot find group field '%s'", key)
				return
			}
			gd.keyIndexes = append(gd.keyIndexes, idx)
		}
	})

	if gd.keyIndexesFailure != nil {
		return nil, gd.keyIndexesFailure
	}

	return gd.keyIndexes, nil
}
