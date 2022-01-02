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
	"sync"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/dubbogo/arana/pkg/runtime/xxast"
)

func TestEval(t *testing.T) {
	v, err := Eval(mustGetMathAtom(), true)
	assert.NoError(t, err, "eval failed")
	t.Log("eval result:", v)
}

func TestCastUnsigned(t *testing.T) {
	first := map[string]interface{}{
		"foo": 10,
		"bar": 1000055,
	}
	v, err := EvalString("$CAST_UNSIGNED(($IF(parseInt(arguments[0].TDDLX_e120910b) == 0 ? null : parseFloat(arguments[0].foo) / parseFloat(arguments[0].bar)>10002, 3.1415, 2.1718)))", first)
	assert.NoError(t, err)
	t.Log("v:", v)

}

func TestEvalString(t *testing.T) {
	v, err := EvalString("$CHAR_LENGTH('你好123')")
	assert.NoError(t, err)
	t.Log("CHAR_LENGTH:", v)
	v, err = EvalString("$LENGTH('你好123')")
	assert.NoError(t, err)
	t.Log("LENGTH:", v)
	v, err = EvalString("$IFNULL(arguments[0],arguments[1])", nil, "1234")

	assert.NoError(t, err)
	t.Log("ISNULL:", v)

	v, err = EvalString("$REPLACE(arguments[0],arguments[1],arguments[2])", "abcabc", "a", "x")
	assert.NoError(t, err)
	t.Log("REPLACE:", v)

	v, err = EvalString("$SUBSTRING(arguments[0],arguments[1],arguments[2])", "abcabc", -4, 3)
	assert.NoError(t, err)
	t.Log("SUBSTRING:", v)

	v, err = EvalString("$REVERSE(arguments[0])", "hello")
	assert.NoError(t, err)
	t.Log("REVERSE:", v)

	v, err = EvalString("$UNIX_TIMESTAMP()", nil)
	assert.NoError(t, err)
	t.Log("UNIX_TIMESTAMP:", v)

	v, err = EvalString("$NOW()", nil)
	assert.NoError(t, err)
	t.Log("NOW:", v)
}

func BenchmarkEval(b *testing.B) {
	atom := mustGetMathAtom()
	args := []interface{}{1}
	wg := &sync.WaitGroup{}
	const warmup = 10000
	wg.Add(warmup)
	for range [warmup]struct{}{} {
		go func() {
			defer wg.Done()
			_, _ = Eval(atom, args...)
		}()
	}
	wg.Wait()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Eval(atom, args...)
		}
	})
}

func mustGetMathAtom() *xxast.MathExpressionAtom {
	stmt, err := xxast.Parse("select * from t where a = 1 + if(?,1,0)")
	if err != nil {
		panic(err.Error())
	}
	sel, _ := stmt.(*xxast.SelectStatement)
	return sel.Where.(*xxast.PredicateExpressionNode).P.(*xxast.BinaryComparisonPredicateNode).Right.(*xxast.AtomPredicateNode).A.(*xxast.MathExpressionAtom)
}
