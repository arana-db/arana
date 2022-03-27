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

package cmp

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

import (
	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

const (
	_       Kind = iota
	Kint         // INT
	Kstring      // STRING
	Kdate        // DATE
)

// NOTICE: DO NOT change orders of following constants!!!
const (
	_    Comparison = iota
	Ceq             // ==
	Cne             // <>
	Cgt             // >
	Cgte            // >=
	Clt             // <
	Clte            // <=
)

var _timeLayouts = []string{
	"2006-01-02",
	"15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04:05.000",
	"Mon Jan 02 15:04:05 MST 2006",
}

var _kindNames = [...]string{
	Kint:    "int",
	Kstring: "string",
	Kdate:   "date",
}

type Kind uint8

func (k Kind) String() string {
	return _kindNames[k]
}

var _comparisonNames = [...]string{
	Cgt:  ">",
	Cgte: ">=",
	Clt:  "<",
	Clte: "<=",
	Ceq:  "=",
	Cne:  "<>",
}

var (
	_comparisonNamesIndex     map[string]Comparison
	_comparisonNamesIndexOnce sync.Once
)

// Comparison represents the comparisons.
type Comparison uint8

func (c Comparison) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(bytesconv.StringToBytes(_comparisonNames[c]))
	return int64(n), err
}

func (c Comparison) String() string {
	return _comparisonNames[c]
}

// ParseComparison parses the Comparison from a string.
func ParseComparison(s string) (c Comparison, ok bool) {
	_comparisonNamesIndexOnce.Do(func() {
		_comparisonNamesIndex = make(map[string]Comparison, len(_comparisonNames)+1)
		for i, v := range _comparisonNames {
			if v == "" {
				continue
			}
			_comparisonNamesIndex[v] = Comparison(i)
		}
		_comparisonNamesIndex["!="] = Cne
	})
	c, ok = _comparisonNamesIndex[s]
	return
}

// Comparative represents a compare.
type Comparative struct {
	k   Kind
	key string
	c   Comparison
	v   string
}

// Kind returns the Kind.
func (c *Comparative) Kind() Kind {
	return c.k
}

// SetKind sets the Kind.
func (c *Comparative) SetKind(k Kind) {
	c.k = k
}

// Key returns the key.
func (c *Comparative) Key() string {
	return c.key
}

func (c *Comparative) String() string {
	var sb strings.Builder
	sb.WriteByte('(')
	sb.WriteString(c.key)
	sb.WriteString(c.c.String())
	_, _ = fmt.Fprintf(&sb, "%v", c.v)
	sb.WriteByte(')')
	return sb.String()
}

// Comparison returns the Comparison.
func (c *Comparative) Comparison() Comparison {
	return c.c
}

// RawValue returns the raw value in string.
func (c *Comparative) RawValue() string {
	return c.v
}

// MustValue returns the value, panic if failed.
func (c *Comparative) MustValue() interface{} {
	v, err := c.Value()
	if err != nil {
		panic(err.Error())
	}
	return v
}

// Value returns auto-converted value.
func (c *Comparative) Value() (interface{}, error) {
	switch c.k {
	case Kint:
		n, err := strconv.ParseInt(c.v, 10, 64)
		if err != nil {
			return nil, err
		}
		return n, nil
	case Kdate:
		d, err := parseDateAuto(c.v)
		if err != nil {
			return nil, err
		}
		return d, nil
	case Kstring:
		return c.v, nil
	default:
		return nil, errors.Errorf("invalid comparative kind %d", c.k)
	}
}

// NewInt64 creates a Comparative from int64.
func NewInt64(key string, comparison Comparison, value int64) *Comparative {
	return New(key, comparison, strconv.FormatInt(value, 10), Kint)
}

// NewDate creates a Comparative from time.Time.
func NewDate(key string, comparison Comparison, value time.Time) *Comparative {
	return New(key, comparison, value.Format("2006-01-02 15:04:05"), Kdate)
}

// NewString creates a Comparative from string.
func NewString(key string, comparison Comparison, value string) *Comparative {
	return New(key, comparison, value, Kstring)
}

// New creates a Comparative.
func New(key string, comparison Comparison, value string, kind Kind) *Comparative {
	return &Comparative{
		key: key,
		k:   kind,
		c:   comparison,
		v:   value,
	}
}

func parseDateAuto(s string) (t time.Time, err error) {
	for _, it := range _timeLayouts {
		t, err = time.ParseInLocation(it, s, time.Local)
		if err == nil {
			return t, nil
		}
	}
	err = errors.Errorf("invalid date string %s", s)
	return
}
