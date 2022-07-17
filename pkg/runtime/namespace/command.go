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

package namespace

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	"github.com/arana-db/arana/pkg/util/log"
)

// UpdateWeight returns a command to update the weight of DB.
func UpdateWeight(group, id string, weight proto.Weight) Command {
	return func(ns *Namespace) error {
		ns.Lock()
		defer ns.Unlock()

		var (
			dss   = ns.dss.Load().(map[string][]proto.DB)
			bingo proto.DB
		)

		if exist, ok := dss[group]; ok {
			for _, it := range exist {
				if it.ID() == id {
					bingo = it
					break
				}
			}
		}

		if bingo == nil {
			log.Errorf("[%s] failed to update weight: no such datasource %s.%s", ns.name, group, id)
			return nil
		}

		if err := bingo.SetWeight(weight); err != nil {
			log.Errorf("[%s] failed to update weight of datasource %s.%s: %v", ns.name, group, id, err)
			return nil
		}

		log.Infof("[%s] update weight of datasource %s.%s successfully", ns.name, group, id)

		return nil
	}
}

// RemoveDB returns a command to remove an existing DB.
func RemoveDB(group, id string) Command {
	return func(ns *Namespace) error {
		ns.Lock()
		defer ns.Unlock()

		var (
			expired proto.DB
			values  []proto.DB
			dss     = ns.dss.Load().(map[string][]proto.DB)
		)

		if exist, ok := dss[group]; ok {
			values = make([]proto.DB, 0, len(exist))
			for _, it := range exist {
				if it.ID() == id {
					expired = it
					continue
				}
				values = append(values, it)
			}
		}

		if expired == nil {
			return nil
		}

		newborn := make(map[string][]proto.DB)
		for k, v := range dss {
			newborn[k] = v
		}
		newborn[group] = values

		// TODO: expire datasource, lazy-close?

		ns.dss.Store(newborn)
		log.Infof("[%s] remove datasource %s.%s successfully", ns.name, group, id)

		return nil
	}
}

// UpsertDB appends a new DB.
func UpsertDB(group string, ds proto.DB) Command {
	return func(ns *Namespace) error {
		ns.Lock()
		defer ns.Unlock()

		var (
			current = ns.dss.Load().(map[string][]proto.DB)
			values  []proto.DB
			expired proto.DB
			id      = ds.ID()
		)

		if exist, ok := current[group]; ok {
			for _, it := range exist {
				if it.ID() == id {
					expired = it
					continue
				}
				values = append(values, it)
			}
		}
		values = append(values, ds)

		if expired != nil {
			// TODO: expire datasource, lazy-close?
			log.Infof("todo: expire DB %s", expired.ID())
		}

		newborn := make(map[string][]proto.DB)
		for k, v := range current {
			newborn[k] = v
		}
		newborn[group] = values

		ns.dss.Store(newborn)

		log.Infof("[%s] upsert db %s.%s successfully", ns.name, group, id)

		return nil
	}
}

// UpdateRule updates the rule.
func UpdateRule(rule *rule.Rule) Command {
	return func(ns *Namespace) error {
		ns.Lock()
		defer ns.Unlock()
		ns.rule.Store(rule)

		return nil
	}
}
