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
 *
 */

package config

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestNodes_Diff(t *testing.T) {
	type args struct {
		old Nodes
	}
	tests := []struct {
		name string
		n    Nodes
		args args
		want *NodesEvent
	}{
		{
			name: "NotChange_Nodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes:    []*Node{},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{},
			},
		},
		{
			name: "Change_AddNodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
				"mock_node_2": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{},
			},
		},
		{
			name: "Change_DeleteNodes",
			n:    map[string]*Node{},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes:    []*Node{},
				UpdateNodes: []*Node{},
				DeleteNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
		},

		{
			name: "Change_UpdateNodes",
			n: map[string]*Node{
				"mock_node_1": {
					Name:     "mock_node_1",
					Host:     "127.0.0.1",
					Port:     3306,
					Username: "arana",
					Password: "arana",
					Database: "mock_db_1",
					Parameters: map[string]string{
						"mock_param_key_1": "mock_param_value_1",
					},
				},
			},
			args: struct{ old Nodes }{
				old: map[string]*Node{
					"mock_node_1": {
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
					},
				},
			},
			want: &NodesEvent{
				AddNodes: []*Node{},
				UpdateNodes: []*Node{
					{
						Name:     "mock_node_1",
						Host:     "127.0.0.1",
						Port:     3306,
						Username: "arana",
						Password: "arana",
						Database: "mock_db_1",
						Parameters: map[string]string{
							"mock_param_key_1": "mock_param_value_1",
						},
					},
				},
				DeleteNodes: []*Node{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.n.Diff(tt.args.old), "Diff(%v)", tt.args.old)
		})
	}
}
