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

package builtin

import (
	"fmt"
	"log"
	"runtime"
	"strings"
)

import (
	"github.com/dop251/goja"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
)

var _ rule.ShardComputer = (*jsShardComputer)(nil)

func init() {
	f := rule.FuncShardComputerFactory(func(columns []string, script string) (rule.ShardComputer, error) {
		return NewJavascriptShardComputer(script, columns[0], columns[1:]...)
	})
	rule.RegisterShardComputer("", f)
	rule.RegisterShardComputer("javascript", f)
	rule.RegisterShardComputer("js", f)
}

const _jsEntrypoint = "__compute__" // shard method name

type jsShardComputer struct {
	// runtime is not thread-safe, wrap as leaky buffer.
	// please see https://go.dev/doc/effective_go#leaky_buffer
	freelist  chan *goja.Runtime
	script    string
	variables []string
}

func (j *jsShardComputer) String() string {
	return j.script
}

// MustNewJavascriptShardComputer returns a shard computer which is based on Javascript, panic if failed.
func MustNewJavascriptShardComputer(script string, column string, otherColumns ...string) rule.ShardComputer {
	c, err := NewJavascriptShardComputer(script, column, otherColumns...)
	if err != nil {
		panic(err.Error())
	}
	return c
}

// NewJavascriptShardComputer returns a shard computer which is based on Javascript.
func NewJavascriptShardComputer(script string, column string, otherColumns ...string) (rule.ShardComputer, error) {
	ret := &jsShardComputer{
		freelist: make(chan *goja.Runtime, runtime.NumCPU()*2),
		script:   generateJavaScript(script, len(otherColumns)+1),
	}
	ret.variables = make([]string, 0, len(otherColumns)+1)
	ret.variables = append(ret.variables, column)
	ret.variables = append(ret.variables, otherColumns...)

	vm, err := createVM(ret.script)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create javascript shard computer")
	}

	ret.freelist <- vm

	return ret, nil
}

func (j *jsShardComputer) Variables() []string {
	return j.variables
}

func (j *jsShardComputer) Compute(values ...proto.Value) (int, error) {
	if len(j.variables) != len(values) {
		return 0, errors.Errorf("the length of params doesn't match: expect=%d, actual=%d", len(j.variables), len(values))
	}

	vm, err := j.getVM()
	if err != nil {
		return 0, err
	}

	defer func() {
		j.putVM(vm)
	}()

	inputs := make([]goja.Value, 0, len(values))
	for i := range values {
		var next interface{}
		switch values[i].Family() {
		case proto.ValueFamilySign:
			next, _ = values[i].Int64()
		case proto.ValueFamilyUnsigned:
			next, _ = values[i].Uint64()
		case proto.ValueFamilyFloat:
			next, _ = values[i].Float64()
		case proto.ValueFamilyDecimal:
			next, _ = values[i].Float64()
		case proto.ValueFamilyBool:
			next, _ = values[i].Bool()
		default:
			next = values[i].String()
		}

		inputs = append(inputs, vm.ToValue(next))
	}

	fn, _ := goja.AssertFunction(vm.Get(_jsEntrypoint))
	res, err := fn(goja.Undefined(), inputs...)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return int(res.ToInteger()), nil
}

func (j *jsShardComputer) getVM() (*goja.Runtime, error) {
	select {
	case next := <-j.freelist:
		return next, nil
	default:
		return createVM(j.script)
	}
}

func (j *jsShardComputer) putVM(vm *goja.Runtime) {
	select {
	case j.freelist <- vm:
	default:
	}
}

func generateJavaScript(script string, columnLen int) string {
	// will generate normalized javascript:
	//
	//   INPUT:
	//     columns:
	//       - school_id
	//       - uid
	//     script: ($0*31+$1) % 32
	//
	//   OUTPUT:
	//     function($0,$1) { // $0=school_id, $1=uid
	//       return ($0*31+$1) % 32;
	//     }
	var sb strings.Builder
	sb.WriteString("function ")
	sb.WriteString(_jsEntrypoint)
	sb.WriteString("(")

	if columnLen > 0 {
		sb.WriteString("$0")
		for i := 1; i < columnLen; i++ {
			_, _ = fmt.Fprintf(&sb, ",$%d", i)
		}
	}

	sb.WriteString(") {\n")

	// NOTICE: compatibility with prev version expression
	script = strings.NewReplacer("$value", "$0").Replace(script)

	if strings.Contains(script, "return ") {
		sb.WriteString(script)
	} else {
		sb.WriteString("return ")
		sb.WriteString(script)
	}

	sb.WriteString("\n}")

	return sb.String()
}

func createVM(script string) (*goja.Runtime, error) {
	vm := goja.New()
	if _, err := vm.RunString(script); err != nil {
		return nil, errors.WithStack(err)
	}

	// TODO: add prelude functions, includes some hash/utils
	_ = vm.Set("log", func(format string, args ...interface{}) {
		log.Printf(format+"\n", args...)
	})

	return vm, nil
}
