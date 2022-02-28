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

package xxast

import (
	"math"
	"strconv"
	"strings"
)

import (
	"github.com/pkg/errors"
)

const (
	_ FunctionArgType = iota
	FunctionArgConstant
	FunctionArgColumn
	FunctionArgExpression
	FunctionArgFunction
	FunctionArgAggrFunction
	FunctionArgCaseWhenElseFunction
	FunctionArgCastFunction
)

const (
	_ FunctionType = iota
	Fudf
	Fscalar
	Fspec
	Fpasswd
)

type FunctionArgType uint8
type FunctionType uint8

func (f FunctionType) String() string {
	switch f {
	case Fudf:
		return "UDF"
	case Fscalar:
		return "SCALAR"
	case Fspec:
		return "SPEC"
	case Fpasswd:
		return "PASSWORD"
	default:
		panic("unreachable")
	}
}

var (
	_ inTablesChecker = (*FunctionArg)(nil)
	_ inTablesChecker = (*Function)(nil)
	_ inTablesChecker = (*AggrFunction)(nil)
	_ inTablesChecker = (*CaseWhenElseFunction)(nil)
	_ inTablesChecker = (*CastFunction)(nil)
)

var (
	_ Restorer = (*FunctionArg)(nil)
	_ Restorer = (*Function)(nil)
	_ Restorer = (*AggrFunction)(nil)
	_ Restorer = (*CaseWhenElseFunction)(nil)
	_ Restorer = (*CastFunction)(nil)
)

type Function struct {
	typ  FunctionType
	name string
	args []*FunctionArg
}

func (f *Function) InTables(tables map[string]struct{}) error {
	for _, it := range f.args {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (f *Function) Type() FunctionType {
	return f.typ
}

func (f *Function) Name() string {
	switch f.typ {
	case Fspec, Fscalar, Fpasswd:
		return strings.ToUpper(f.name)
	default:
		return f.name
	}
}

func (f *Function) Args() []*FunctionArg {
	return f.args
}

func (f *Function) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString(f.Name())
	sb.WriteByte('(')

	if n := len(f.args); n > 0 {
		if err := f.args[0].Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
		for i := 1; i < n; i++ {
			sb.WriteString(", ")
			if err := f.args[i].Restore(sb, args); err != nil {
				return errors.WithStack(err)
			}
		}
	}

	sb.WriteByte(')')
	return nil
}

type FunctionArg struct {
	typ   FunctionArgType
	value interface{}
}

func (f *FunctionArg) InTables(tables map[string]struct{}) error {
	switch f.typ {
	case FunctionArgColumn:
		return ColumnNameExpressionAtom(f.value.([]string)).InTables(tables)
	case FunctionArgExpression:
		return f.value.(ExpressionNode).InTables(tables)
	case FunctionArgConstant:
		return nil
	case FunctionArgFunction:
		return f.value.(*Function).InTables(tables)
	case FunctionArgAggrFunction:
		return f.value.(*AggrFunction).InTables(tables)
	case FunctionArgCaseWhenElseFunction:
		return f.value.(*CaseWhenElseFunction).InTables(tables)
	case FunctionArgCastFunction:
		return f.value.(*CastFunction).InTables(tables)
	default:
		panic("unreachable")
	}
}

func (f *FunctionArg) Type() FunctionArgType {
	return f.typ
}

func (f *FunctionArg) Value() interface{} {
	return f.value
}

func (f *FunctionArg) Restore(sb *strings.Builder, args *[]int) error {
	var restorer Restorer
	switch f.typ {
	case FunctionArgColumn:
		restorer = f.value.(ColumnNameExpressionAtom)
	case FunctionArgExpression:
		restorer = f.value.(ExpressionNode)
	case FunctionArgConstant:
		restorer = ConstantExpressionAtom{Inner: f.value}
	case FunctionArgFunction:
		restorer = f.value.(*Function)
	case FunctionArgAggrFunction:
		restorer = f.value.(*AggrFunction)
	case FunctionArgCaseWhenElseFunction:
		restorer = f.value.(*CaseWhenElseFunction)
	case FunctionArgCastFunction:
		restorer = f.value.(*CastFunction)
	default:
		panic("unreachable")
	}

	if err := restorer.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

const (
	_flagAggrCountStar AggrFunctionFlag = 1 << iota
)

const (
	AggrAvg   = "AVG"
	AggrMax   = "MAX"
	AggrMin   = "MIN"
	AggrSum   = "SUM"
	AggrCount = "COUNT"
)

const (
	AggregatorAll      = "ALL"
	AggregatorDistinct = "DISTINCT"
)

type AggrFunctionFlag uint8

type AggrFunction struct {
	flag       AggrFunctionFlag
	name       string
	aggregator string
	args       []*FunctionArg
}

func (af *AggrFunction) InTables(tables map[string]struct{}) error {
	for _, it := range af.args {
		if err := it.InTables(tables); err != nil {
			return err
		}
	}
	return nil
}

func (af *AggrFunction) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString(af.name)
	sb.WriteByte('(')
	if af.IsCountStar() {
		sb.WriteByte('*')
		sb.WriteByte(')')
		return nil
	}

	if len(af.aggregator) > 0 {
		sb.WriteString(af.aggregator)
		sb.WriteByte(' ')
	}

	if len(af.args) < 1 {
		sb.WriteByte(')')
		return nil
	}

	if err := af.args[0].Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	for i := 1; i < len(af.args); i++ {
		sb.WriteString(", ")
		if err := af.args[i].Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}
	sb.WriteByte(')')
	return nil
}

func (af *AggrFunction) Aggreator() (string, bool) {
	if len(af.aggregator) < 1 {
		return "", false
	}
	return af.aggregator, true
}

func (af *AggrFunction) Name() string {
	return af.name
}

func (af *AggrFunction) Args() []*FunctionArg {
	return af.args
}

func (af *AggrFunction) IsCountStar() bool {
	return af.flag&_flagAggrCountStar != 0
}

func (af *AggrFunction) EnableCountStar() {
	af.flag |= _flagAggrCountStar
}

func NewAggrFunction(name string, aggregator string, args []*FunctionArg) *AggrFunction {
	return &AggrFunction{
		name:       name,
		aggregator: aggregator,
		args:       args,
	}
}

type CaseWhenElseFunction struct {
	caseBlock ExpressionNode
	branches  [][2]*FunctionArg
	elseBlock *FunctionArg
}

func (c *CaseWhenElseFunction) InTables(tables map[string]struct{}) error {
	if c.caseBlock != nil {
		if err := c.caseBlock.InTables(tables); err != nil {
			return err
		}
	}

	for _, branch := range c.branches {
		for _, it := range branch[:] {
			if err := it.InTables(tables); err != nil {
				return err
			}
		}
	}

	if c.elseBlock != nil {
		if err := c.elseBlock.InTables(tables); err != nil {
			return err
		}
	}

	return nil
}

func (c *CaseWhenElseFunction) Case() ExpressionNode {
	return c.caseBlock
}

func (c *CaseWhenElseFunction) Branches() [][2]*FunctionArg {
	return c.branches
}

func (c *CaseWhenElseFunction) Else() (*FunctionArg, bool) {
	if c.elseBlock != nil {
		return c.elseBlock, true
	}
	return nil, false
}

func (c *CaseWhenElseFunction) Restore(sb *strings.Builder, args *[]int) error {
	sb.WriteString("CASE")

	if c.caseBlock != nil {
		sb.WriteByte(' ')
		if err := c.caseBlock.Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	for _, it := range c.branches {
		sb.WriteString(" WHEN ")
		if err := it[0].Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
		sb.WriteString(" THEN ")
		if err := it[1].Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	if c.elseBlock != nil {
		sb.WriteString(" ELSE ")
		if err := c.elseBlock.Restore(sb, args); err != nil {
			return errors.WithStack(err)
		}
	}

	sb.WriteString(" END")
	return nil
}

type castFunctionFlag uint8

const (
	castFunctionFlagCast castFunctionFlag = 1 << iota
	castFunctionFlagCharsetName
)

type CastFunction struct {
	flag castFunctionFlag
	src  ExpressionNode
	cast interface{} // *ConvertDataType or string
}

func (c *CastFunction) InTables(tables map[string]struct{}) error {
	return c.src.InTables(tables)
}

func (c *CastFunction) Source() ExpressionNode {
	return c.src
}

func (c *CastFunction) GetCharset() (string, bool) {
	charset, ok := c.cast.(string)
	return charset, ok
}

func (c *CastFunction) GetCast() (*ConvertDataType, bool) {
	t, ok := c.cast.(*ConvertDataType)
	return t, ok
}

func (c *CastFunction) Restore(sb *strings.Builder, args *[]int) error {
	if c.flag&castFunctionFlagCast != 0 {
		sb.WriteString("CAST")
	} else {
		sb.WriteString("CONVERT")
	}
	sb.WriteByte('(')
	if err := c.src.Restore(sb, args); err != nil {
		return errors.WithStack(err)
	}

	switch cast := c.cast.(type) {
	case string:
		sb.WriteString(" USING ")
		sb.WriteString(cast)
	case *ConvertDataType:
		if c.flag&castFunctionFlagCast != 0 {
			sb.WriteString(" AS ")
		} else {
			sb.WriteString(", ")
		}
		cast.writeTo(sb)
	}
	sb.WriteByte(')')

	return nil
}

const (
	_ CastType = iota
	CastToBinary
	CastToNChar
	CastToChar
	CastToDate
	CastToDateTime
	CastToTime
	CastToJson
	CastToDecimal
	CastToSigned
	CastToUnsigned
	CastToSignedInteger
	CastToUnsignedInteger
)

type CastType uint8

func (c CastType) String() string {
	switch c {
	case CastToBinary:
		return "BINARY"
	case CastToNChar:
		return "NCHAR"
	case CastToChar:
		return "CHAR"
	case CastToDate:
		return "DATE"
	case CastToDateTime:
		return "DATETIME"
	case CastToTime:
		return "TIME"
	case CastToJson:
		return "JSON"
	case CastToDecimal:
		return "DECIMAL"
	case CastToSigned:
		return "SIGNED"
	case CastToUnsigned:
		return "UNSIGNED"
	case CastToSignedInteger:
		return "SIGNED INTEGER"
	case CastToUnsignedInteger:
		return "UNSIGNED INTEGER"
	default:
		panic("unreachable")
	}
}

type ConvertDataType struct {
	typ                    CastType
	dimension0, dimension1 int64
	charset                string
}

func (cd *ConvertDataType) Charset() (string, bool) {
	if len(cd.charset) < 1 {
		return "", false
	}
	return cd.charset, true
}

func (cd *ConvertDataType) Dimensions() (int64, int64) {
	return cd.dimension0, cd.dimension1
}

func (cd *ConvertDataType) Type() CastType {
	return cd.typ
}

func (cd *ConvertDataType) String() string {
	var sb strings.Builder
	cd.writeTo(&sb)
	return sb.String()
}

func (cd *ConvertDataType) writeTo(sb *strings.Builder) {
	sb.WriteString(cd.typ.String())
	switch cd.typ {
	case CastToSigned, CastToUnsigned, CastToSignedInteger, CastToUnsignedInteger:
	case CastToBinary, CastToNChar:
		if cd.dimension0 != math.MinInt64 {
			sb.WriteByte(' ')
			sb.WriteByte('(')
			sb.WriteString(strconv.FormatInt(cd.dimension0, 10))
			sb.WriteByte(')')
		}
	case CastToChar:
		if cd.dimension0 != math.MinInt64 {
			sb.WriteByte(' ')
			sb.WriteByte('(')
			sb.WriteString(strconv.FormatInt(cd.dimension0, 10))
			sb.WriteByte(')')
		}
		if len(cd.charset) > 0 {
			sb.WriteString(" CHARSET ")
			sb.WriteString(cd.charset)
		}
	case CastToDecimal:
		if cd.dimension0 != math.MinInt64 && cd.dimension1 != math.MinInt64 {
			sb.WriteByte('(')
			sb.WriteString(strconv.FormatInt(cd.dimension0, 10))
			sb.WriteByte(',')
			sb.WriteString(strconv.FormatInt(cd.dimension1, 10))
			sb.WriteByte(')')
		}
	}
}
