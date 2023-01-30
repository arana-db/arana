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
	"context"
	"fmt"
)

import (
	"github.com/blang/semver"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
)

var _funcWarehouse map[string]Func

// RegisterFunc registers a mysql Func.
func RegisterFunc(name string, f Func) {
	if _funcWarehouse == nil {
		_funcWarehouse = make(map[string]Func)
	}
	_funcWarehouse[name] = f
	log.Debugf("register mysql function %s ok", name)
}

// GetFunc gets Func by given name.
func GetFunc(name string) (Func, bool) {
	found, ok := _funcWarehouse[name]
	return found, ok
}

// MustGetFunc gets Func by given name, panic if missing.
func MustGetFunc(name string) Func {
	ret, ok := GetFunc(name)
	if !ok {
		panic(fmt.Sprintf("no such mysql function '%s'!", name))
	}
	return ret
}

// Valuer represents a generator or value.
type Valuer interface {
	// Value computes and returns the Value.
	Value(ctx context.Context) (Value, error)
}

type FuncValuer func(ctx context.Context) (Value, error)

func (f FuncValuer) Value(ctx context.Context) (Value, error) {
	return f(ctx)
}

type directValuer struct {
	v Value
}

func (t directValuer) Value(ctx context.Context) (Value, error) {
	return t.v, nil
}

// ToValuer wraps Value to Valuer directly.
func ToValuer(value Value) Valuer {
	return directValuer{v: value}
}

// Func represents a MySQL function.
type Func interface {
	// Apply call the current function.
	Apply(ctx context.Context, inputs ...Valuer) (Value, error)

	// NumInput returns the minimum number of inputs.
	NumInput() int
}

// VersionedFunc represents a MySQL function with versions.
// See this doc: https://dev.mysql.com/doc/refman/8.0/en/built-in-function-reference.html
type VersionedFunc interface {
	Func

	// Versions returns the version range of current function.
	Versions() semver.Range
}

// ValidateFunction checks the function compatibility from server version.
func ValidateFunction(ctx context.Context, f Func, strict bool) bool {
	vf, ok := f.(VersionedFunc)
	if !ok { // omit if not versioned function
		return true
	}

	serverVersion, ok := ctx.Value(ContextKeyServerVersion{}).(string)
	if !ok {
		return !strict
	}

	ver, err := semver.Parse(serverVersion)
	if err != nil {
		return !strict
	}

	return vf.Versions()(ver)
}
