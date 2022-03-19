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

package function

import (
	"crypto/md5"
	"crypto/sha1"
	"embed"
	"fmt"
	"hash"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

import (
	"github.com/cespare/xxhash/v2"

	"github.com/dop251/goja"

	"github.com/lestrrat-go/strftime"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/runtime/misc"
	"github.com/arana-db/arana/pkg/util/bytesconv"
)

//go:embed scripts
var scripts embed.FS

var _decimalRegex = regexp.MustCompile(`^(?P<sign>[+\-])?(?P<l>[0-9])+(?P<r>\.[0-9]+)$`)

var (
	freeList = make(chan *VM, 16)
)

const FuncUnary = "__unary"

type VM struct {
	inner    *goja.Runtime
	existing map[uint64]struct{}
}

func NewVM() *VM {
	jsVm := goja.New()

	jsVm.SetRandSource(func() float64 {
		return float64(time.Now().UnixNano())
	})

	toValue := jsVm.ToValue

	toString := func(input interface{}) (string, bool) {
		if input == nil {
			return "", false
		}

		switch v := input.(type) {
		case string:
			return v, true
		case time.Time:
			return v.UTC().Format("2006-01-02 15:04:05"), true
		}

		return fmt.Sprintf("%v", input), true
	}

	_ = jsVm.Set("__curtime", func() string {
		return time.Now().Local().Format("15:04:05")
	})

	_ = jsVm.Set("__curdate", func() string {
		return time.Now().Local().Format("2006-01-02")
	})

	_ = jsVm.Set("__month", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}

		return toValue(int(dt.Month()))
	})

	_ = jsVm.Set("__monthname", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Month().String())
	})

	_ = jsVm.Set("__day", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Day())
	})

	_ = jsVm.Set("__dayname", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Weekday().String())
	})

	_ = jsVm.Set("__dayofweek", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(int(dt.Weekday()) + 1)
	})

	_ = jsVm.Set("__week", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		_, w := dt.ISOWeek()
		return toValue(w)
	})

	_ = jsVm.Set("__dayofmonth", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Day())
	})

	_ = jsVm.Set("__dayofyear", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.YearDay())
	})

	_ = jsVm.Set("__quarter", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		var q int
		switch dt.Month() {
		case time.December, time.November, time.October:
			q = 4
		case time.September, time.August, time.July:
			q = 3
		case time.June, time.May, time.April:
			q = 2
		case time.March, time.February, time.January:
			q = 1
		}
		return toValue(q)
	})

	_ = jsVm.Set("__hour", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Hour())
	})

	_ = jsVm.Set("__minute", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Minute())
	})

	_ = jsVm.Set("__second", func(t interface{}) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.Second())
	})

	_ = jsVm.Set("__datediff", func(t1, t2 interface{}) goja.Value {
		a, ok := parseTime(t1)
		if !ok {
			return goja.Null()
		}
		b, ok := parseTime(t2)
		if !ok {
			return goja.Null()
		}
		return toValue(a.Sub(b).Hours() / 24)
	})

	_ = jsVm.Set("__adddate", func(t interface{}, days int) goja.Value {
		dt, ok := parseTime(t)
		if !ok {
			return goja.Null()
		}
		v := dt.AddDate(0, 0, days)
		return toValue(v.UnixNano() / 1e6)
	})

	_ = jsVm.Set("__parse_time", func(s string) goja.Value {
		dt, ok := parseTime(s)
		if !ok {
			return goja.Null()
		}
		return toValue(dt.UnixNano() / 1e6)
	})

	_ = jsVm.Set("__dateformat", func(first interface{}, pattern string) goja.Value {
		dt, ok := parseTime(first)
		if !ok {
			return goja.Null()
		}
		s, err := strftime.Format(pattern, dt)
		if err != nil {
			return goja.Null()
		}
		return toValue(s)
	})

	toHash := func(gen func() hash.Hash) func(interface{}) goja.Value {
		return func(src interface{}) goja.Value {
			s, ok := toString(src)
			if !ok {
				return goja.Null()
			}
			h := gen()
			_, _ = io.WriteString(h, s)
			return toValue(fmt.Sprintf("%x", h.Sum(nil)))
		}
	}

	_ = jsVm.Set(FuncUnary, func(op string, v interface{}) goja.Value {
		v, err := misc.ComputeUnary(op, v)
		if err != nil {
			return goja.Null()
		}
		return toValue(v)
	})

	_ = jsVm.Set("__md5", toHash(md5.New))
	_ = jsVm.Set("__sha", toHash(sha1.New))
	_ = jsVm.Set("__sha1", toHash(sha1.New))

	_ = jsVm.Set("__ltrim", func(s string) string {
		return strings.TrimLeftFunc(s, func(r rune) bool {
			return unicode.IsSpace(r)
		})
	})

	_ = jsVm.Set("__rtrim", func(s string) string {
		return strings.TrimRightFunc(s, func(r rune) bool {
			return unicode.IsSpace(r)
		})
	})

	_ = jsVm.Set("__to_string", func(v interface{}) goja.Value {
		s, ok := toString(v)
		if !ok {
			return goja.Null()
		}
		return toValue(s)
	})

	_ = jsVm.Set("__lpad", func(s string, n int, pad string) string {
		return misc.PadLeft(s, pad, n)
	})
	_ = jsVm.Set("__rpad", func(s string, n int, pad string) string {
		return misc.PadRight(s, pad, n)
	})

	_ = jsVm.Set("__cast_charset", func(input interface{}) goja.Value {
		s, ok := toString(input)
		if !ok {
			return goja.Null()
		}
		switch strings.ToUpper(s) {
		case "UTF8":
		}
		return toValue(s)
	})

	_ = jsVm.Set("__cast_decimal", func(m, d int, s string) goja.Value {
		if !_decimalRegex.MatchString(s) {
			return goja.Null()
		}

		subs := _decimalRegex.FindStringSubmatch(s)
		keys := _decimalRegex.SubexpNames()
		if len(subs) != len(keys) {
			return goja.Null()
		}

		var (
			neg   bool
			left  string
			right string
		)
		for i := 1; i < len(keys); i++ {
			sub := subs[i]
			switch keys[i] {
			case "sign":
				if sub == "-" {
					neg = true
				}
			case "l":
				left = sub
			case "r":
				right = sub[1:]
			}
		}

		// TODO: handle overflow
		if len(left) > m-d {
			return goja.Null()
		}

		if len(right) > d {
			right = right[:d]
		}

		var sb strings.Builder
		if neg {
			sb.WriteByte('-')
		}
		sb.WriteString(left)
		if strings.TrimRight(right, "0") != "" {
			sb.WriteByte('.')
			sb.WriteString(right)
		}
		f, err := strconv.ParseFloat(sb.String(), 64)
		if err != nil {
			return goja.Null()
		}
		return toValue(f)
	})

	ret := &VM{
		inner:    jsVm,
		existing: make(map[uint64]struct{}),
	}

	if err := ret.loadScripts(); err != nil {
		panic(err)
	}

	return ret
}

func (vm *VM) loadScripts() error {
	ent, err := scripts.ReadDir("scripts")
	if err != nil {
		return errors.Wrap(err, "read scripts folder failed")
	}
	for _, it := range ent {
		if it.IsDir() || !strings.HasSuffix(it.Name(), ".js") {
			continue
		}
		var data []byte
		if data, err = scripts.ReadFile(fmt.Sprintf("scripts/%s", it.Name())); err != nil {
			return err
		}
		if _, err = vm.inner.RunString(bytesconv.BytesToString(data)); err != nil {
			return err
		}
	}
	return nil
}

func (vm *VM) Eval(script string, args []interface{}) (interface{}, error) {
	id := xxhash.Sum64String(script)

	// init script
	if _, ok := vm.existing[id]; !ok {
		if err := vm.register(id, script); err != nil {
			return nil, err
		}
		vm.existing[id] = struct{}{}
	}

	var (
		fnName    = vm.getScriptName(id)
		arguments = make([]goja.Value, 0, len(args))
	)

	for _, it := range args {
		arguments = append(arguments, vm.inner.ToValue(it))
	}

	fn, ok := goja.AssertFunction(vm.inner.Get(fnName))
	if !ok {
		return nil, errors.Errorf("no script found!")
	}

	v, err := fn(goja.Undefined(), arguments...)
	if err != nil {
		return nil, errors.Wrapf(err, "execute the script '%s' failed: arguments=%v", script, args)
	}

	if goja.IsNaN(v) || goja.IsInfinity(v) {
		return math.NaN(), nil
	}

	return v.Export(), nil
}

func (vm *VM) getScriptName(id uint64) string {
	var sb strings.Builder
	vm.writeScriptName(&sb, id)
	return sb.String()
}

func (vm *VM) writeScriptName(sb *strings.Builder, id uint64) {
	sb.WriteString("$_")
	sb.WriteString(strconv.FormatUint(id, 16))
}

func (vm *VM) register(id uint64, script string) error {
	var sb strings.Builder

	sb.WriteString("function ")
	vm.writeScriptName(&sb, id)
	sb.WriteString("() {\n  return ")
	sb.WriteString(script)
	sb.WriteString(";\n}")

	_, err := vm.inner.RunString(sb.String())

	return err
}

func parseTime(input interface{}) (t time.Time, ok bool) {
	switch val := input.(type) {
	case time.Time:
		t = val
		ok = true
	case string:
		for _, layout := range []string{
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if dt, err := time.ParseInLocation(layout, val, time.Local); err == nil {
				t = dt
				ok = true
				break
			}
		}
	}
	return
}

func BorrowVM() *VM {
	var vm *VM
	select {
	case vm = <-freeList:
	default:
		vm = NewVM()
	}
	return vm
}

func ReturnVM(vm *VM) {
	select {
	case freeList <- vm:
	default:
	}
}
