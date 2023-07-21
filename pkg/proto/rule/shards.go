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

package rule

import (
	"fmt"
	"strings"
)

import (
	"github.com/google/btree"
)

func IntersectionShards(first, second *Shards, others ...*Shards) *Shards {
	var (
		ret    = NewShards()
		visits map[uint64]struct{}
	)

	h := func(db, tb uint32) bool {
		k := uint64(db)<<32 | uint64(tb)
		if visits == nil {
			visits = make(map[uint64]struct{})
		}
		if _, ok := visits[k]; ok {
			ret.Add(db, tb)
		} else {
			visits[k] = struct{}{}
		}
		return true
	}

	first.Each(h)
	second.Each(h)
	for _, next := range others {
		next.Each(h)
	}
	return ret
}

func UnionShards(first, second *Shards, others ...*Shards) *Shards {
	ret := NewShards()

	h := func(db, tb uint32) bool {
		ret.Add(db, tb)
		return true
	}
	first.Each(h)
	second.Each(h)

	for _, next := range others {
		next.Each(h)
	}

	return ret
}

type Shards btree.BTree

func NewShards() *Shards {
	return (*Shards)(btree.New(2))
}

func (sd *Shards) Remove(db, table uint32, otherTables ...uint32) {
	(*btree.BTree)(sd).Delete(shard(uint64(db)<<32 | uint64(table)))
	for _, next := range otherTables {
		(*btree.BTree)(sd).Delete(shard(uint64(db)<<32 | uint64(next)))
	}
}

func (sd *Shards) Add(db, table uint32, otherTables ...uint32) {
	(*btree.BTree)(sd).ReplaceOrInsert(shard(uint64(db)<<32 | uint64(table)))
	for _, next := range otherTables {
		(*btree.BTree)(sd).ReplaceOrInsert(shard(uint64(db)<<32 | uint64(next)))
	}
}

func (sd *Shards) Each(h func(db, tb uint32) bool) {
	if sd == nil {
		return
	}
	(*btree.BTree)(sd).Ascend(func(i btree.Item) bool {
		s := i.(shard)
		return h(s.getDB(), s.getTable())
	})
}

func (sd *Shards) Min() (db, tb uint32, ok bool) {
	min := (*btree.BTree)(sd).Min()
	if min == nil {
		return
	}
	s := min.(shard)
	db, tb, ok = s.getDB(), s.getTable(), true
	return
}

func (sd *Shards) Max() (db, tb uint32, ok bool) {
	max := (*btree.BTree)(sd).Max()
	if max == nil {
		return
	}
	s := max.(shard)
	db, tb, ok = s.getDB(), s.getTable(), true
	return
}

func (sd *Shards) Len() int {
	return (*btree.BTree)(sd).Len()
}

func (sd *Shards) String() string {
	if sd == nil {
		return "*"
	}

	var sb strings.Builder
	sb.WriteByte('[')
	prev := int64(-1)
	sd.Each(func(db, tb uint32) bool {
		if prev != int64(db) {
			if prev != -1 {
				sb.WriteByte(';')
			}
			_, _ = fmt.Fprint(&sb, db)
			prev = int64(db)
			sb.WriteByte(':')
		} else {
			sb.WriteByte(',')
		}
		_, _ = fmt.Fprint(&sb, tb)
		return true
	})
	sb.WriteByte(']')
	return sb.String()
}

type shard uint64

func (s shard) Less(than btree.Item) bool {
	that := than.(shard)
	db0, db1 := s.getDB(), that.getDB()

	if db0 == db1 {
		return s.getTable() < that.getTable()
	}

	return db0 < db1
}

func (s shard) getDB() uint32 {
	return uint32(s >> 32)
}

func (s shard) getTable() uint32 {
	return uint32(s)
}
