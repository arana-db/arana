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

package ddl

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

import (
	gxbig "github.com/dubbogo/gost/math/big"

	"github.com/pkg/errors"
)

import (
	errors2 "github.com/arana-db/arana/pkg/mysql/errors"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/resultx"
	"github.com/arana-db/arana/pkg/runtime/ast"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/runtime/misc/extvalue"
	"github.com/arana-db/arana/pkg/runtime/plan"
	"github.com/arana-db/arana/pkg/util/log"
)

var _ proto.Plan = (*SetVariablePlan)(nil)

type SetVariablePlan struct {
	plan.BasePlan
	Stmt *ast.SetStatement
}

func (d *SetVariablePlan) Type() proto.PlanType {
	return proto.PlanTypeExec
}

func (d *SetVariablePlan) ExecIn(ctx context.Context, conn proto.VConn) (proto.Result, error) {
	ctx, span := plan.Tracer.Start(ctx, "SetVariablePlan.ExecIn")
	defer span.End()

	// 0. generate newest variables to be updated
	nextVars, err := d.nextVars()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	tVars := rcontext.TransientVariables(ctx)

	if tVars == nil {
		return nil, errors.New("cannot find transient variables map")
	}

	// 1. generate snapshot, will recovery if update failed
	snapshot := make(map[string]interface{})
	for k, v := range tVars {
		k, v := k, v
		snapshot[k] = v
	}

	// 2. update persist variables
	for k := range nextVars {
		tVars[k] = nextVars[k]
	}

	// 3. touch a sql, will trigger variables sync, here use 'SELECT 1'
	res, err := conn.Query(ctx, "", "SELECT 1")
	if err != nil {
		// OOPS: variables sync failed, time-machine bingo!
		for k := range tVars {
			delete(tVars, k)
		}
		for k, v := range snapshot {
			k, v := k, v
			tVars[k] = v
		}

		log.Debugf("recovery snapshot because sync variables failure")

		// unwrap mysql error
		if sqlErr, ok := errors.Cause(err).(*errors2.SQLError); ok {
			return nil, sqlErr
		}

		return nil, err
	}

	// exhause datasets
	if ds, err := res.Dataset(); err == nil {
		defer ds.Close()
		for {
			if _, err = ds.Next(); err != nil {
				break
			}
		}
	}

	// 4. return a fake result
	return resultx.New(), nil
}

func (d *SetVariablePlan) nextVars() (map[string]interface{}, error) {
	ret := make(map[string]interface{})
	var key strings.Builder
	for _, next := range d.Stmt.Variables {
		v, err := extvalue.GetValueFromAtom(next.Value, d.Args)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if next.Global {
			// TODO: implement global sync
			return nil, errors.New("setting of global variable is not unsupported yet")
		}

		var val interface{}
		switch it := v.(type) {
		case int8, uint8, int16, uint16, int32, uint32, uint64, int, uint:
			var n int64
			if n, err = strconv.ParseInt(fmt.Sprintf("%d", it), 10, 64); err != nil {
				return nil, errors.WithStack(err)
			}
			val = n
		case bool:
			if it {
				val = int64(1)
			} else {
				val = int64(0)
			}
		case float32:
			val = float64(it)
		case int64, float64, string:
			val = it
		case *gxbig.Decimal:
			s := it.String()
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				val = n
				break
			}
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				val = f
				break
			}
			return nil, errors.Errorf("cannot process decimal variable: %s", s)
		default:
			return nil, errors.Errorf("unsupported variable type %T", it)
		}

		key.WriteByte('@')
		if next.System {
			key.WriteByte('@')
		}
		key.WriteString(next.Name)

		ret[key.String()] = val
		key.Reset()
	}
	return ret, nil
}
