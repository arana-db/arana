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
	"fmt"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewExprShardComputer(t *testing.T) {
	type args struct {
		expr   string
		column string
	}
	tests := []struct {
		name    string
		args    args
		want    rule.ShardComputer
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"NewExprShardComputer",
			args{"hash(toint(substr(#uid#, 1, 2)), 100)", "uid"},
			&exprShardComputer{"hash(toint(substr(#uid#, 1, 2)), 100)", "uid"},
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewExprShardComputer(tt.args.expr, tt.args.column)
			if !tt.wantErr(t, err, fmt.Sprintf("NewExprShardComputer(%v, %v)", tt.args.expr, tt.args.column)) {
				return
			}
			assert.Equalf(t, tt.want, got, "NewExprShardComputer(%v, %v)", tt.args.expr, tt.args.column)
		})
	}
}

func Test_exprShardComputer_Compute(t *testing.T) {
	type fields struct {
		expr   string
		column string
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			"Compute_1",
			fields{"hash(toint(substr(#uid#, 1, 2)), 100)", "uid"},
			args{87616},
			87,
			assert.NoError,
		},
		{
			"Compute_2",
			fields{"hash(concat(#uid#, '1'), 100)", "uid"},
			args{87616},
			61,
			assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compute := &exprShardComputer{
				expr:   tt.fields.expr,
				column: tt.fields.column,
			}
			got, err := compute.Compute(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("Compute(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Compute(%v)", tt.args.value)
		})
	}
}
