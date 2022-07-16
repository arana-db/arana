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
	"container/list"
	"database/sql"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"testing"
)

import (
	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

import (
	consts "github.com/arana-db/arana/pkg/constants/mysql"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/mysql/rows"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/util/rand2"
	"github.com/arana-db/arana/testdata"
)

type fakePojo struct {
	ID     int64
	Name   string
	Gender int64
}

func scanPojo(row proto.Row, dest *fakePojo) error {
	s := make([]proto.Value, 3)
	if err := row.Scan(s); err != nil {
		return err
	}

	var (
		id     sql.NullInt64
		name   sql.NullString
		gender sql.NullInt64
	)
	_, _, _ = id.Scan(s[0]), name.Scan(s[1]), gender.Scan(s[2])

	dest.ID = id.Int64
	dest.Name = name.String
	dest.Gender = gender.Int64

	return nil
}

func generateFakeParallelDataset(ctrl *gomock.Controller, pairs ...int) RandomAccessDataset {
	fields := []proto.Field{
		mysql.NewField("id", consts.FieldTypeLong),
		mysql.NewField("name", consts.FieldTypeVarChar),
		mysql.NewField("gender", consts.FieldTypeLong),
	}

	getFakeDataset := func(offset, n int) proto.Dataset {
		l := list.New()
		for i := 0; i < n; i++ {
			r := rows.NewTextVirtualRow(fields, []proto.Value{
				int64(offset + i),
				fmt.Sprintf("Fake %d", offset+i),
				rand2.Int63n(2),
			})
			l.PushBack(r)
		}

		ds := testdata.NewMockDataset(ctrl)

		ds.EXPECT().Close().Return(nil).AnyTimes()
		ds.EXPECT().Fields().Return(fields, nil).AnyTimes()
		ds.EXPECT().Next().
			DoAndReturn(func() (proto.Row, error) {
				head := l.Front()
				if head == nil {
					return nil, io.EOF
				}
				l.Remove(head)
				return head.Value.(proto.Row), nil
			}).
			AnyTimes()

		return ds
	}

	var gens []GenerateFunc

	for i := 1; i < len(pairs); i += 2 {
		offset := pairs[i-1]
		n := pairs[i]
		gens = append(gens, func() (proto.Dataset, error) {
			return getFakeDataset(offset, n), nil
		})
	}

	ret, _ := Parallel(gens[0], gens[1:]...)
	return ret
}

func TestParallelDataset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pd := generateFakeParallelDataset(ctrl, 0, 3, 3, 3, 6, 3)

	var pojo fakePojo

	row, err := pd.PeekN(0)
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	assert.Equal(t, int64(0), pojo.ID)

	row, err = pd.PeekN(1)
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	assert.Equal(t, int64(3), pojo.ID)

	row, err = pd.PeekN(2)
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	assert.Equal(t, int64(6), pojo.ID)

	err = pd.SetNextN(1)
	assert.NoError(t, err)

	row, err = pd.Peek()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	assert.Equal(t, int64(3), pojo.ID)

	row, err = pd.Next()
	assert.NoError(t, err)
	assert.NoError(t, scanPojo(row, &pojo))
	assert.Equal(t, int64(3), pojo.ID)

	var cnt int

	_ = scanPojo(row, &pojo)
	t.Logf("first: %#v\n", pojo)
	cnt++

	for {
		row, err = pd.Next()
		if err != nil {
			break
		}

		_ = scanPojo(row, &pojo)
		t.Logf("next: %#v\n", pojo)
		cnt++
	}

	assert.Equal(t, 9, cnt)
}

func TestParallelDataset_SortBy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pairs := []int{
		5, 8, // offset, size
		0, 3,
		2, 4,
		6, 2,
	}

	var (
		d = generateFakeParallelDataset(ctrl, pairs...)

		ids sort.IntSlice
	)

	for {
		bingo := -1

		var (
			wg   sync.WaitGroup
			lock sync.Mutex
			min  int64 = math.MaxInt64
		)

		wg.Add(d.Len())

		// search the next minimum value in parallel
		for i := 0; i < d.Len(); i++ {
			i := i
			go func() {
				defer wg.Done()

				r, err := d.PeekN(i)
				if err == io.EOF {
					return
				}

				var pojo fakePojo
				err = scanPojo(r, &pojo)
				assert.NoError(t, err)

				lock.Lock()
				defer lock.Unlock()
				if pojo.ID < min {
					min = pojo.ID
					bingo = i
				}
			}()
		}

		wg.Wait()

		if bingo == -1 {
			break
		}

		err := d.SetNextN(bingo)
		assert.NoError(t, err)

		var pojo fakePojo

		r, err := d.Next()
		assert.NoError(t, err)
		err = scanPojo(r, &pojo)
		assert.NoError(t, err)

		t.Logf("next: %#v\n", pojo)
		ids = append(ids, int(pojo.ID))
	}

	assert.True(t, sort.IsSorted(ids), "values should be sorted")
}
