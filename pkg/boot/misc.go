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
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/util/math"
)

var (
	_regexpTopology     *regexp.Regexp
	_regexpTopologyOnce sync.Once
)

func getTopologyRegexp() *regexp.Regexp {
	_regexpTopologyOnce.Do(func() {
		_regexpTopology = regexp.MustCompile(`\${(?P<begin>\d+)\.{2,}(?P<end>\d+)}`)
	})
	return _regexpTopology
}

func parseTopology(input string) (format string, begin, end int, err error) {
	mats := getTopologyRegexp().FindAllStringSubmatch(input, -1)

	if len(mats) < 1 {
		format = input
		begin = -1
		end = -1
		return
	}

	if len(mats) > 1 {
		err = errors.Errorf("invalid topology expression: %s", input)
		return
	}

	var beginStr, endStr string
	for i := 1; i < len(mats[0]); i++ {
		switch getTopologyRegexp().SubexpNames()[i] {
		case "begin":
			beginStr = mats[0][i]
		case "end":
			endStr = mats[0][i]
		}
	}

	if len(beginStr) != len(endStr) {
		err = errors.Errorf("invalid topology expression: %s", input)
		return
	}

	format = getTopologyRegexp().ReplaceAllString(input, fmt.Sprintf(`%%0%dd`, len(beginStr)))
	begin, _ = strconv.Atoi(strings.TrimLeft(beginStr, "0"))
	end, _ = strconv.Atoi(strings.TrimLeft(endStr, "0"))
	return
}

func makeVTable(tableName string, table *config.Table) (*rule.VTable, error) {
	var (
		vt                 rule.VTable
		topology           rule.Topology
		dbFormat, tbFormat string
		dbBegin, tbBegin   int
		dbEnd, tbEnd       int
		err                error
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

	dbAmount := dbEnd - dbBegin + 1
	tbAmount := tbEnd - tbBegin + 1
	tblsPerDB := tbAmount / dbAmount

	for i := 0; i < dbAmount; i++ {
		var (
			x = dbBegin + i
			y []int
		)
		for j := 0; j < tblsPerDB; j++ {
			y = append(y, tbBegin+tblsPerDB*i+j)
		}

		topology.SetTopology(x, y...)
	}

	defaultSteps := math.Max(dbAmount, tbAmount)

	dbSm, err := toShardMetadata(table.DbRules, defaultSteps)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse db rules")
	}
	tbSm, err := toShardMetadata(table.TblRules, defaultSteps)
	if err != nil {
		return nil, errors.Wrap(err, "cannot parse table rules")
	}

	for i := 0; i < math.Max(len(dbSm), len(tbSm)); i++ {
		var vs rule.VShard
		if i < len(dbSm) {
			vs.DB = dbSm[i]
		}
		if i < len(tbSm) {
			vs.Table = tbSm[i]
		}
		vt.AddVShards(&vs)
	}

	allowFullScan, err := strconv.ParseBool(table.Attributes["allow_full_scan"])
	if err == nil && allowFullScan {
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

func toSharder(input *config.Rule) (rule.ShardComputer, error) {
	columns := make([]string, 0, len(input.Columns))
	for i := range input.Columns {
		columns = append(columns, input.Columns[i].Name)
	}
	return rule.NewComputer(input.Type, columns, input.Expr)
}

func getRender(format string) func(int) string {
	if strings.ContainsRune(format, '%') {
		return func(i int) string {
			return fmt.Sprintf(format, i)
		}
	}
	return func(i int) string {
		return format
	}
}

func toShardMetadata(rules []*config.Rule, defaultSteps int) ([]*rule.ShardMetadata, error) {
	toShardColumn := func(ru *config.ColumnRule, dst *[]*rule.ShardColumn, defaultSteps int) {
		unit := rule.Unum
		switch strings.ToLower(ru.Type) {
		case "string", "str":
			unit = rule.Ustr
		case "year":
			unit = rule.Uyear
		case "month":
			unit = rule.Umonth
		case "week":
			unit = rule.Uweek
		case "day":
			unit = rule.Uday
		case "hour":
			unit = rule.Uhour
		}
		c := &rule.ShardColumn{
			Name:  ru.Name,
			Steps: ru.Step,
			Stepper: rule.Stepper{
				N: 1,
				U: unit,
			},
		}

		if c.Steps == 0 {
			c.Steps = defaultSteps
		}

		*dst = append(*dst, c)
	}

	var ret []*rule.ShardMetadata
	for i := range rules {
		ru := rules[i]
		c, err := toSharder(ru)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		var shardColumns []*rule.ShardColumn
		for _, next := range ru.Columns {
			toShardColumn(next, &shardColumns, defaultSteps)
		}
		ret = append(ret, &rule.ShardMetadata{
			ShardColumns: shardColumns,
			Computer:     c,
		})
	}
	return ret, nil
}
