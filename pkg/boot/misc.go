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

package boot

import (
	"regexp"
	"sync"

	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/pkg/errors"
)

func makeVTable(tableName string, table *config.Table) (*rule.VTable, error) {
	var (
		vt                 rule.VTable
		topology           rule.Topology
		dbFormat, tbFormat string
		dbBegin, tbBegin   int
		dbEnd, tbEnd       int
		err                error
		ok                 bool
	)

	if table.Topology != nil {
		if len(table.Topology.DbPattern) > 0 {
			if dbFormat, dbBegin, dbEnd, err = parseTopology(table.Topology.DbPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		if len(table.Topology.TblPattern) > 0 {
			if tbFormat, tbBegin, tbEnd, err = parseTopology(table.Topology.TblPattern); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	}
	topology.SetRender(getRender(dbFormat), getRender(tbFormat))

	var (
		keys                 map[string]struct{}
		dbSharder, tbSharder map[string]rule.ShardComputer
		dbSteps, tbSteps     map[string]int
	)
	for _, it := range table.DbRules {
		var shd rule.ShardComputer
		if shd, err = toSharder(it); err != nil {
			return nil, err
		}
		if dbSharder == nil {
			dbSharder = make(map[string]rule.ShardComputer)
		}
		if keys == nil {
			keys = make(map[string]struct{})
		}
		if dbSteps == nil {
			dbSteps = make(map[string]int)
		}
		columnKey := it.ColumnKey()
		dbSharder[columnKey] = shd
		keys[columnKey] = struct{}{}
		dbSteps[columnKey] = it.Step
	}

	for _, it := range table.TblRules {
		var shd rule.ShardComputer
		if shd, err = toSharder(it); err != nil {
			return nil, err
		}
		if tbSharder == nil {
			tbSharder = make(map[string]rule.ShardComputer)
		}
		if keys == nil {
			keys = make(map[string]struct{})
		}
		if tbSteps == nil {
			tbSteps = make(map[string]int)
		}
		columnKey := it.ColumnKey()
		tbSharder[columnKey] = shd
		keys[columnKey] = struct{}{}
		tbSteps[columnKey] = it.Step
	}

	for k := range keys {
		var (
			shd                    rule.ShardComputer
			dbMetadata, tbMetadata *rule.ShardMetadata
		)
		if shd, ok = dbSharder[k]; ok {
			dbMetadata = &rule.ShardMetadata{
				Computer: shd,
				Stepper:  rule.DefaultNumberStepper,
			}
			if s, ok := dbSteps[k]; ok && s > 0 {
				dbMetadata.Steps = s
			} else if dbBegin >= 0 && dbEnd >= 0 {
				dbMetadata.Steps = 1 + dbEnd - dbBegin
			}
		}
		if shd, ok = tbSharder[k]; ok {
			tbMetadata = &rule.ShardMetadata{
				Computer: shd,
				Stepper:  rule.DefaultNumberStepper,
			}
			if s, ok := tbSteps[k]; ok && s > 0 {
				tbMetadata.Steps = s
			} else if tbBegin >= 0 && tbEnd >= 0 {
				tbMetadata.Steps = 1 + tbEnd - tbBegin
			}
		}
		vt.SetShardMetadata(k, dbMetadata, tbMetadata)

		tpRes := make(map[int][]int)
		step := tbMetadata.Steps
		if dbMetadata.Steps > step {
			step = dbMetadata.Steps
		}
		rng, _ := tbMetadata.Stepper.Ascend(0, step)
		for rng.HasNext() {
			var (
				seed  = rng.Next()
				dbIdx = -1
				tbIdx = -1
			)
			if dbMetadata != nil {
				if dbIdx, err = dbMetadata.Computer.Compute(seed); err != nil {
					return nil, errors.WithStack(err)
				}
			}
			if tbMetadata != nil {
				if tbIdx, err = tbMetadata.Computer.Compute(seed); err != nil {
					return nil, errors.WithStack(err)
				}
			}
			tpRes[dbIdx] = append(tpRes[dbIdx], tbIdx)
		}

		for dbIndex, tbIndexes := range tpRes {
			topology.SetTopology(dbIndex, tbIndexes...)
		}
	}

	if table.AllowFullScan {
		vt.SetAllowFullScan(true)
	}
	if table.Sequence != nil {
		vt.SetAutoIncrement(&rule.AutoIncrement{
			Type:   table.Sequence.Type,
			Option: table.Sequence.Option,
		})
	}

	// TODO: process attributes
	_ = table.Attributes["sql_max_limit"]

	vt.SetTopology(&topology)
	vt.SetName(tableName)

	return &vt, nil
}

var (
	_fullTableNameRegexp     *regexp.Regexp
	_fullTableNameRegexpOnce sync.Once
)

func parseDatabaseAndTable(name string) (db, tbl string, err error) {
	_fullTableNameRegexpOnce.Do(func() {
		_fullTableNameRegexp = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_-]+)\.([a-zA-Z_][a-zA-Z0-9_-]+)$`)
	})

	matches := _fullTableNameRegexp.FindStringSubmatch(name)
	switch len(matches) {
	case 3:
		db = matches[1]
		tbl = matches[2]
	default:
		err = errors.Errorf("invalid full table format: %s", name)
	}
	return
}
