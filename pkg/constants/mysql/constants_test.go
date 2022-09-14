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

package mysql

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestIsNum(t *testing.T) {
	type args struct {
		typ uint8
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Test_1", args{9}, true},
		{"Test_2", args{8}, true},
		{"Test_3", args{7}, false},
		{"Test_4", args{13}, true},
		{"Test_5", args{246}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsNum(tt.args.typ), "IsNum(%v)", tt.args.typ)
		})
	}
}
