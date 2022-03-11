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

package ast

import (
	"strings"
)

import (
	"github.com/pkg/errors"
)

var (
	_ Statement = (*DescribeStatement)(nil)
	_ Statement = (*ExplainStatement)(nil)
)

const (
	modeDesc describeMode = iota
	modeDescribe
	modeExplain
)

const (
	modeDescStr     = "DESC"
	modeDescribeStr = "DESCRIBE"
	modeExplainStr  = "EXPLAIN"
)

type describeMode uint8

func (d *describeMode) parse(input string) error {
	switch strings.ToUpper(input) {
	case modeDescStr:
		*d = modeDesc
	case modeDescribeStr:
		*d = modeDescribe
	case modeExplainStr:
		*d = modeExplain
	default:
		return errors.Errorf("invalid describe string %s", input)
	}
	return nil
}

func (d describeMode) String() string {
	switch d {
	case modeDesc:
		return modeDescStr
	case modeDescribe:
		return modeDescribeStr
	case modeExplain:
		return modeExplainStr
	default:
		panic("unreachable")
	}
}

type DescribeStatement struct {
	mode   describeMode
	table  TableName
	column string
}

func (d *DescribeStatement) Validate() error {
	return nil
}

func (d *DescribeStatement) CntParams() int {
	return 0
}

func (d *DescribeStatement) GetSQLType() SQLType {
	return Squery
}

func (d *DescribeStatement) Table() TableName {
	return d.table
}

func (d *DescribeStatement) Column() (string, bool) {
	if len(d.column) > 0 {
		return d.column, true
	}
	return "", false
}

func (d *DescribeStatement) Describe() string {
	return d.mode.String()
}

type ExplainStatement struct {
	mode describeMode
	tgt  Statement
}

func (e *ExplainStatement) Validate() error {
	return e.tgt.Validate()
}

func (e *ExplainStatement) Target() Statement {
	return e.tgt
}

func (e *ExplainStatement) GetSQLType() SQLType {
	return Squery
}

func (e *ExplainStatement) Explain() string {
	return e.mode.String()
}
