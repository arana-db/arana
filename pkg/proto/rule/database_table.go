// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package rule

import (
	"sort"
	"strings"
)

// DatabaseTable represents the pair of database and table.
type DatabaseTable struct {
	Database, Table string
}

// DatabaseTables represents a bundle of databases and tables.
type DatabaseTables map[string][]string

// IsConfused returns weather the database tables contains conflicts.
func (dt DatabaseTables) IsConfused() bool {
	if dt == nil {
		return false
	}

	var (
		dbs = make(map[string]struct{})
		tbs = make(map[string]struct{})
	)
	for db, tbls := range dt {
		dbs[db] = struct{}{}
		for _, tbl := range tbls {
			if _, ok := tbs[tbl]; ok {
				return true
			}
			tbs[tbl] = struct{}{}
		}
	}
	return false
}

// Smallest returns the smallest pair of database and table.
func (dt DatabaseTables) Smallest() (db, tbl string) {
	for k := range dt {
		if db == "" || strings.Compare(k, db) == -1 {
			db = k
		}
	}
	for _, it := range dt[db] {
		if tbl == "" || strings.Compare(it, tbl) == -1 {
			tbl = it
		}
	}
	return
}

// Or returns the union of two DatabaseTables.
func (dt DatabaseTables) Or(other DatabaseTables) DatabaseTables {
	if dt == nil || other == nil {
		return nil
	}
	if dt.IsEmpty() {
		return other
	}
	if other.IsEmpty() {
		return other
	}

	indexes := make(map[string]map[string]struct{})

	for _, next := range [2]DatabaseTables{
		dt, other,
	} {
		for db, tbls := range next {
			for _, tbl := range tbls {
				if exist, ok := indexes[db]; ok {
					exist[tbl] = struct{}{}
				} else {
					indexes[db] = map[string]struct{}{tbl: {}}
				}
			}
		}
	}

	if _, ok := indexes["*"]["*"]; ok {
		return nil
	}

	ret := make(DatabaseTables)

	fuzz := indexes["*"]
	delete(indexes, "*")

	for db, v := range indexes {
		if _, ok := v["*"]; ok {
			ret[db] = append(ret[db], "*")
			continue
		}

		for tbl := range v {
			if _, ok := fuzz[tbl]; ok { // 假如已经星号匹配了,直接忽略
				continue
			}
			ret[db] = append(ret[db], tbl)
		}
		sort.Strings(ret[db])
	}

	if len(fuzz) > 0 {
		s := make([]string, 0, len(fuzz))
		for it := range fuzz {
			s = append(s, it)
		}
		sort.Strings(s)
		ret["*"] = s
	}

	return ret
}

// And returns the intersection of two DatabaseTables.
func (dt DatabaseTables) And(other DatabaseTables) DatabaseTables {
	if dt == nil {
		return other
	}
	if other == nil {
		return dt
	}

	const (
		flagA uint8 = 0x01
		flagB uint8 = 0x01 << 1
	)

	merge := make(map[string]map[string]uint8)
	for db, tbs := range dt {
		for _, tb := range tbs {
			if exist, ok := merge[db]; ok {
				exist[tb] = flagA
			} else {
				merge[db] = map[string]uint8{
					tb: flagA,
				}
			}
		}
	}
	for db, tbs := range other {
		for _, tb := range tbs {
			if exist, ok := merge[db]; ok {
				exist[tb] |= flagB
			} else {
				merge[db] = map[string]uint8{
					tb: flagB,
				}
			}
		}
	}

	ret := make(DatabaseTables)

	dbFuzz := merge["*"]
	delete(merge, "*")

	if f := dbFuzz["*"]; f&flagA != 0 && f&flagB != 0 {
		return nil
	} else if f&flagA != 0 {
		return other
	} else if f&flagB != 0 {
		return dt
	}

	for db, tbls := range merge {
		fuzz := tbls["*"]
		delete(tbls, "*")

		// 子库全匹配
		if fuzz&flagA != 0 && fuzz&flagB != 0 {
			ret[db] = append(ret[db], "*")
			continue
		}

		if fuzz&flagA != 0 {
			for t, f := range dbFuzz {
				switch t {
				case "*":
				default:
					if f&flagB != 0 {
						ret[db] = append(ret[db], t)
					}
				}
			}
		} else if fuzz&flagB != 0 {
			for t, f := range dbFuzz {
				switch t {
				case "*":
				default:
					if f&flagA != 0 {
						ret[db] = append(ret[db], t)
					}
				}
			}
		}

		for tbl, flag := range tbls {
			var (
				fa = flag&flagA != 0
				fb = flag&flagB != 0
			)

			if fa && fb {
				ret[db] = append(ret[db], tbl)
				continue
			}

			if (fa && fuzz&flagB != 0) || (fb && fuzz&flagA != 0) {
				ret[db] = append(ret[db], tbl)
				continue
			}

			if len(dbFuzz) < 1 {
				continue
			}

			fuzz = dbFuzz[tbl]
			if (fa && fuzz&flagB != 0) || (fb && fuzz&flagA != 0) {
				ret[db] = append(ret[db], tbl)
				continue
			}

			fuzz = dbFuzz["*"]
			if (fa && fuzz&flagB != 0) || (fb && fuzz&flagA != 0) {
				ret[db] = append(ret[db], tbl)
				continue
			}
		}
	}

	return ret
}

// IsFullScan returns true if the current DatabaseTables will cause full scan.
func (dt DatabaseTables) IsFullScan() bool {
	if dt == nil {
		return true
	}
	for k, v := range dt {
		if k == "*" {
			return true
		}
		for _, it := range v {
			if it == "*" {
				return true
			}
		}
	}

	return false
}

// IsEmpty returns true if the current DatabaseTables is empty.
func (dt DatabaseTables) IsEmpty() bool {
	return dt != nil && len(dt) == 0
}

func (dt DatabaseTables) String() string {
	if dt.IsFullScan() {
		return `["*"]`
	}
	if dt.IsEmpty() {
		return "[]"
	}

	var keys []string
	for k := range dt {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	var i int

	sb.WriteByte('[')
	for _, db := range keys {
		for _, tb := range dt[db] {
			if i > 0 {
				sb.WriteByte(',')
				sb.WriteByte(' ')
			}
			sb.WriteByte('"')
			sb.WriteString(db)
			sb.WriteByte('.')
			sb.WriteString(tb)
			sb.WriteByte('"')
			i++
		}
	}
	sb.WriteByte(']')
	return sb.String()
}
