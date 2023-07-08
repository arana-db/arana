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

package proto

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	perrors "github.com/pkg/errors"

	"github.com/shopspring/decimal"
)

const (
	_ ValueFamily = iota
	ValueFamilyString
	ValueFamilySign
	ValueFamilyUnsigned
	ValueFamilyFloat
	ValueFamilyDecimal
	ValueFamilyBool
	ValueFamilyTime
	ValueFamilyDuration // TODO: support HH:mm::ss, used by INTERVAL
)

var _valueFamilyNames = [...]struct {
	name     string
	numberic bool
}{
	ValueFamilyString:   {"STRING", false},
	ValueFamilySign:     {"SIGNED", true},
	ValueFamilyUnsigned: {"UNSIGNED", true},
	ValueFamilyFloat:    {"FLOAT", true},
	ValueFamilyDecimal:  {"DECIMAL", true},
	ValueFamilyBool:     {"BOOL", true},
	ValueFamilyTime:     {"TIME", false},
	ValueFamilyDuration: {"DURATION", false},
}

type Null struct{}

func (n Null) String() string {
	return "NULL"
}

type ValueFamily uint8

func (v ValueFamily) IsNumberic() bool {
	return _valueFamilyNames[v].numberic
}

func (v ValueFamily) String() string {
	return _valueFamilyNames[v].name
}

// Value represents the cell value of Row.
type Value interface {
	fmt.Stringer
	Family() ValueFamily
	Float64() (float64, error)
	Int64() (int64, error)
	Uint64() (uint64, error)
	Decimal() (decimal.Decimal, error)
	Bool() (bool, error)
	Time() (time.Time, error)
	Less(than Value) bool
}

var (
	_ Value = (*int64Value)(nil)
	_ Value = (*stringValue)(nil)
	_ Value = (*float64Value)(nil)
	_ Value = (*decimalValue)(nil)
	_ Value = (*uint64Value)(nil)
	_ Value = (*boolValue)(nil)
	_ Value = (*timeValue)(nil)
)

func MustNewValue(input interface{}) Value {
	v, err := NewValue(input)
	if err != nil {
		panic(err.Error())
	}
	return v
}

func NewValue(input interface{}) (Value, error) {
	if input == nil {
		return nil, nil
	}
	switch v := input.(type) {
	case Null, *Null:
		return nil, nil
	case int8:
		return NewValueInt64(int64(v)), nil
	case uint8:
		return NewValueUint64(uint64(v)), nil
	case int16:
		return NewValueInt64(int64(v)), nil
	case uint16:
		return NewValueUint64(uint64(v)), nil
	case int32:
		return NewValueInt64(int64(v)), nil
	case uint32:
		return NewValueUint64(uint64(v)), nil
	case int64:
		return NewValueInt64(v), nil
	case uint64:
		return NewValueUint64(v), nil
	case int:
		return NewValueInt64(int64(v)), nil
	case uint:
		return NewValueUint64(uint64(v)), nil
	case string:
		return NewValueString(v), nil
	case bool:
		return NewValueBool(v), nil
	case time.Time:
		return NewValueTime(v), nil
	case float32:
		return NewValueFloat64(float64(v)), nil
	case float64:
		return NewValueFloat64(v), nil
	case *decimal.Decimal:
		return NewValueDecimal(*v), nil
	case decimal.Decimal:
		return NewValueDecimal(v), nil
	case decimal.NullDecimal:
		if !v.Valid {
			return nil, nil
		}
		return NewValueDecimal(v.Decimal), nil
	case *decimal.NullDecimal:
		if !v.Valid {
			return nil, nil
		}
		return NewValueDecimal(v.Decimal), nil
	case *gxbig.Decimal:
		if v == nil {
			return nil, nil
		}
		return MustNewValueDecimalString(v.String()), nil
	case gxbig.Decimal:
		return MustNewValueDecimalString(v.String()), nil
	default:
		return nil, perrors.Errorf("unsupported constant value type: %T", v)
	}
}

type int64Value int64

func NewValueInt64(v int64) Value {
	return int64Value(v)
}

func (i int64Value) Time() (time.Time, error) {
	return time.Time{}, perrors.New("cannot convert int64 to time")
}

func (i int64Value) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (i int64Value) Uint64() (uint64, error) {
	return uint64(i), nil
}

func (i int64Value) Bool() (bool, error) {
	return int64(i) != 0, nil
}

func (i int64Value) Family() ValueFamily {
	return ValueFamilySign
}

func (i int64Value) String() string {
	return strconv.FormatInt(int64(i), 10)
}

func (i int64Value) Float64() (float64, error) {
	return float64(i), nil
}

func (i int64Value) Int64() (int64, error) {
	return int64(i), nil
}

func (i int64Value) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromInt(int64(i)), nil
}

type float64Value float64

func NewValueFloat64(v float64) Value {
	return float64Value(v)
}

func (f float64Value) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (f float64Value) Time() (time.Time, error) {
	return time.Time{}, perrors.New("cannot convert float64 to time")
}

func (f float64Value) Uint64() (uint64, error) {
	return uint64(f), nil
}

func (f float64Value) Bool() (bool, error) {
	// FIXME: float64 compare zero
	return float64(f) != 0, nil
}

func (f float64Value) String() string {
	return strconv.FormatFloat(float64(f), 'g', -1, 64)
}

func (f float64Value) Family() ValueFamily {
	return ValueFamilyFloat
}

func (f float64Value) Float64() (float64, error) {
	return float64(f), nil
}

func (f float64Value) Int64() (int64, error) {
	return int64(f), nil
}

func (f float64Value) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromFloat(float64(f)), nil
}

type stringValue string

func NewValueString(s string) Value {
	return stringValue(s)
}

func (s stringValue) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (s stringValue) Time() (t time.Time, err error) {
	input := string(s)

	if input == "" {
		return
	}

	if t, err = time.Parse("2006-01-02", input); err == nil {
		return t, nil
	}

	if t, err = time.Parse("2006-01-02 15:04:05", input); err == nil {
		return t, nil
	}

	if t, err = time.Parse("2006-01-02 15:04:05.000", input); err == nil {
		return t, nil
	}

	if t, err = time.Parse("2006-01-02 15:04:05.000000", input); err == nil {
		return t, nil
	}

	err = perrors.Errorf("cannot parse time from '%s'", input)
	return
}

func (s stringValue) Uint64() (uint64, error) {
	return strconv.ParseUint(string(s), 10, 64)
}

func (s stringValue) Bool() (bool, error) {
	return strings.TrimFunc(string(s), func(r rune) bool {
		return unicode.IsSpace(r) || r == 0
	}) != "", nil
}

func (s stringValue) String() string {
	return string(s)
}

func (s stringValue) Family() ValueFamily {
	return ValueFamilyString
}

func (s stringValue) Float64() (float64, error) {
	return strconv.ParseFloat(string(s), 64)
}

func (s stringValue) Int64() (int64, error) {
	i := strings.Index(string(s), ".")
	if i == -1 {
		return strconv.ParseInt(string(s), 10, 64)
	}
	return strconv.ParseInt(string(s[:i]), 10, 64)
}

func (s stringValue) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromString(string(s))
}

type decimalValue decimal.Decimal

func NewValueDecimal(d decimal.Decimal) Value {
	return decimalValue(d)
}

func MustNewValueDecimalString(s string) Value {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err.Error())
	}
	return NewValueDecimal(d)
}

func (d decimalValue) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (d decimalValue) Time() (time.Time, error) {
	return time.Time{}, perrors.New("cannot convert decimal to time")
}

func (d decimalValue) Bool() (bool, error) {
	return !decimal.Decimal(d).IsZero(), nil
}

func (d decimalValue) Uint64() (uint64, error) {
	return uint64(decimal.Decimal(d).IntPart()), nil
}

func (d decimalValue) String() string {
	return decimal.Decimal(d).String()
}

func (d decimalValue) Family() ValueFamily {
	return ValueFamilyDecimal
}

func (d decimalValue) Float64() (float64, error) {
	return decimal.Decimal(d).InexactFloat64(), nil
}

func (d decimalValue) Int64() (int64, error) {
	return decimal.Decimal(d).IntPart(), nil
}

func (d decimalValue) Decimal() (decimal.Decimal, error) {
	return decimal.Decimal(d), nil
}

type uint64Value uint64

func NewValueUint64(v uint64) Value {
	return uint64Value(v)
}

func (u uint64Value) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (u uint64Value) Time() (time.Time, error) {
	return time.Time{}, perrors.Errorf("cannot convert uint64 to time")
}

func (u uint64Value) String() string {
	return strconv.FormatUint(uint64(u), 10)
}

func (u uint64Value) Family() ValueFamily {
	return ValueFamilyUnsigned
}

func (u uint64Value) Float64() (float64, error) {
	return float64(u), nil
}

func (u uint64Value) Int64() (int64, error) {
	return int64(u), nil
}

func (u uint64Value) Uint64() (uint64, error) {
	return uint64(u), nil
}

func (u uint64Value) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromString(u.String())
}

func (u uint64Value) Bool() (bool, error) {
	return u != 0, nil
}

type boolValue bool

func NewValueBool(b bool) Value {
	return boolValue(b)
}

func (b boolValue) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (b boolValue) Time() (time.Time, error) {
	return time.Time{}, perrors.New("cannot convert bool to time")
}

func (b boolValue) String() string {
	if b {
		return "1"
	}
	return "0"
}

func (b boolValue) Family() ValueFamily {
	return ValueFamilyBool
}

func (b boolValue) Float64() (float64, error) {
	if b {
		return 1, nil
	}
	return 0, nil
}

func (b boolValue) Int64() (int64, error) {
	if b {
		return 1, nil
	}
	return 0, nil
}

func (b boolValue) Uint64() (uint64, error) {
	if b {
		return 1, nil
	}
	return 0, nil
}

func (b boolValue) Decimal() (decimal.Decimal, error) {
	if b {
		return decimal.NewFromInt(1), nil
	}
	return decimal.Zero, nil
}

func (b boolValue) Bool() (bool, error) {
	return bool(b), nil
}

type typedValue struct {
	Value
	typed ValueFamily
}

func NewValueTyped(origin Value, overwriteFamily ValueFamily) Value {
	return typedValue{
		Value: origin,
		typed: overwriteFamily,
	}
}

func (tv typedValue) Family() ValueFamily {
	return tv.typed
}

type timeValue time.Time

func NewValueTime(t time.Time) Value {
	return timeValue(t)
}

func (t timeValue) Less(than Value) bool {
	// TODO implement me
	panic("implement me")
}

func (t timeValue) String() string {
	return time.Time(t).Format("2006-01-02 15:04:05")
}

func (t timeValue) Family() ValueFamily {
	return ValueFamilyTime
}

func (t timeValue) Float64() (float64, error) {
	f, _ := strconv.ParseFloat(time.Time(t).Format("20060102150405"), 64)
	return f, nil
}

func (t timeValue) Int64() (int64, error) {
	n, _ := strconv.ParseInt(time.Time(t).Format("20060102150405"), 10, 64)
	return n, nil
}

func (t timeValue) Uint64() (uint64, error) {
	n, _ := strconv.ParseUint(time.Time(t).Format("20060102150405"), 10, 64)
	return n, nil
}

func (t timeValue) Decimal() (decimal.Decimal, error) {
	return decimal.NewFromString(time.Time(t).Format("20060102150405"))
}

func (t timeValue) Bool() (bool, error) {
	return true, nil
}

func (t timeValue) Time() (time.Time, error) {
	return time.Time(t), nil
}

func CompareValue(a, b Value) int {
	if a == nil || b == nil {
		panic(fmt.Sprintf("cannot compare %v and %v!", a, b))
	}

	switch {
	case a.Family().IsNumberic() || b.Family().IsNumberic():
		x, ex := a.Decimal()
		if ex != nil {
			x = decimal.Zero
		}
		y, ey := b.Decimal()
		if ey != nil {
			y = decimal.Zero
		}

		switch {
		case x.LessThan(y):
			return -1
		case x.GreaterThan(y):
			return 1
		default:
			return 0
		}
	default:
		return strings.Compare(a.String(), b.String())
	}
}
