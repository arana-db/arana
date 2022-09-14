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
	"reflect"
)

func (u *User) Equals(o *User) bool {

	return u.Username == o.Username && u.Password == o.Password
}

func (n *Node) Equals(o *Node) bool {
	if n.Name != o.Name {
		return false
	}

	if n.Host != o.Host || n.Port != o.Port {
		return false
	}

	if n.Database != o.Database || n.Username != o.Username || n.Password != o.Password {
		return false
	}

	if n.Weight != o.Weight {
		return false
	}

	if len(n.Labels) != len(o.Labels) {
		return false
	}

	if len(n.Labels) != len(o.Labels) || !reflect.DeepEqual(n.Labels, o.Labels) {
		return false
	}

	if len(n.Parameters) != len(o.Parameters) || !reflect.DeepEqual(n.Parameters, o.Parameters) {
		return false
	}

	return true
}

func (r Rules) Equals(o Rules) bool {
	if len(r) == 0 && len(o) == 0 {
		return true
	}

	if len(r) != len(o) {
		return false
	}

	newT := make([]*Rule, 0, 4)
	updateT := make([]*Rule, 0, 4)
	deleteT := make([]*Rule, 0, 4)

	newTmp := map[string]*Rule{}
	oldTmp := map[string]*Rule{}

	for i := range r {
		newTmp[r[i].Column] = r[i]
	}
	for i := range o {
		oldTmp[o[i].Column] = o[i]
	}

	for i := range r {
		if _, ok := oldTmp[o[i].Column]; !ok {
			newT = append(newT, o[i])
		}
	}

	for i := range o {
		val, ok := newTmp[o[i].Column]
		if !ok {
			deleteT = append(deleteT, o[i])
			continue
		}

		if !reflect.DeepEqual(val, o[i]) {
			updateT = append(updateT, val)
			continue
		}
	}

	return len(newT) == 0 && len(updateT) == 0 && len(deleteT) == 0
}

func (t *Table) Equals(o *Table) bool {
	if len(t.DbRules) != len(o.DbRules) {
		return false
	}

	if len(t.TblRules) != len(o.TblRules) {
		return false
	}

	if !Rules(t.DbRules).Equals(o.DbRules) {
		return false
	}
	if !Rules(t.TblRules).Equals(o.TblRules) {
		return false
	}

	if !reflect.DeepEqual(t.Topology, o.Topology) || !reflect.DeepEqual(t.ShadowTopology, o.ShadowTopology) {
		return false
	}

	if t.AllowFullScan == o.AllowFullScan {
		return false
	}

	if !reflect.DeepEqual(t.Attributes, o.Attributes) {
		return false
	}

	return true
}
