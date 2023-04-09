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

package rule

import (
	"runtime"
	"strings"
)

import (
	"github.com/dop251/goja"

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/proto/rule"
)

var _ rule.ShardComputer = (*jsShardComputer)(nil)

const (
	_jsEntrypoint   = "__compute__" // shard method name
	_jsVariableName = "$"           // variable name of column in sharding script
)

type jsShardComputer struct {
	// runtime is not thread-safe, wrap as leaky buffer.
	// please see https://go.dev/doc/effective_go#leaky_buffer
	freelist chan *goja.Runtime
	script   string
}

// NewJavascriptShardComputer returns a shard computer which is based on Javascript.
func NewJavascriptShardComputer(script string, columns []string) (rule.ShardComputer, error) {
	script = wrapScript(script, columns)

	vm, err := createVM(script)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create javascript shard computer")
	}

	ret := &jsShardComputer{
		freelist: make(chan *goja.Runtime, runtime.NumCPU()*2),
		script:   script,
	}
	ret.freelist <- vm

	return ret, nil
}

func (j *jsShardComputer) Compute(value interface{}) (int, error) {
	vm, err := j.getVM()
	if err != nil {
		return 0, err
	}

	defer func() {
		j.putVM(vm)
	}()

	params, ok := value.([]interface{})
	if !ok {
		return 0, errors.Wrapf(err, "javascript shard computer params type not is []string")
	}
	gojaValues := make([]goja.Value, 0, len(params))
	for _, v := range params {
		gojaValues = append(gojaValues, vm.ToValue(v))
	}

	fn, _ := goja.AssertFunction(vm.Get(_jsEntrypoint))
	res, err := fn(goja.Undefined(), gojaValues...)
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

func wrapScript(script string, columns []string) string {
	params := make([]string, 0, len(columns))
	for i := range columns {
		params = append(params, _jsVariableName+columns[i])
	}
	jsFuncParams := strings.Join(params, ",")

	var sb strings.Builder

	sb.Grow(32 + len(_jsEntrypoint) + len(jsFuncParams) + len(script))

	sb.WriteString("function ")
	sb.WriteString(_jsEntrypoint)
	sb.WriteString("(")
	sb.WriteString(jsFuncParams)
	sb.WriteString(") {\n")

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

	return vm, nil
}
