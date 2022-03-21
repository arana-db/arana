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

package namespace

import (
	"context"
	"io"
	"sort"
	"sync"
)

import (
	"github.com/pkg/errors"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/pkg/util/rand2"
)

var _namespaces sync.Map

// Load loads a namespace, return nil if no namespace found.
func Load(namespace string) *Namespace {
	exist, ok := _namespaces.Load(namespace)
	if !ok {
		return nil
	}
	return exist.(*Namespace)
}

// Register registers a namespace.
func Register(namespace *Namespace) error {
	name := namespace.Name()
	if _, loaded := _namespaces.LoadOrStore(name, namespace); loaded {
		return errors.Errorf("cannot register namesapce: conflict name %s", name)
	}
	return nil
}

// Unregister unregisters a namespace.
func Unregister(namespace string) error {
	removed, loaded := _namespaces.LoadAndDelete(namespace)
	if !loaded {
		return nil
	}
	return removed.(*Namespace).Close()
}

type (
	// Namespace represents a logical database with all resources.
	Namespace struct {
		sync.Mutex

		closed atomic.Bool

		name string // the name of Namespace

		rule      atomic.Value // *rule.Rule
		optimizer proto.Optimizer

		// datasource map, eg: employee_0001 -> [mysql-a,mysql-b,mysql-c], ... employee_0007 -> [mysql-x,mysql-y,mysql-z]
		dss atomic.Value // map[string][]proto.DB

		cmds chan Command  // command queue
		done chan struct{} // done notify
	}

	// Command represents the command to control Namespace.
	Command func(ns *Namespace)
)

// New creates a Namespace.
func New(name string, optimizer proto.Optimizer, commands ...Command) *Namespace {
	ns := &Namespace{
		name:      name,
		optimizer: optimizer,
		cmds:      make(chan Command, 1),
		done:      make(chan struct{}),
	}
	ns.dss.Store(make(map[string][]proto.DB)) // init empty map
	ns.rule.Store(&rule.Rule{})               // init empty rule

	for _, cmd := range commands {
		cmd(ns)
	}

	go ns.loopCmds()

	return ns
}

// Name returns the name of namespace.
func (ns *Namespace) Name() string {
	return ns.name
}

// DBGroups returns the group names of DB.
func (ns *Namespace) DBGroups() []string {
	// FIXME: consider cache it
	dss := ns.dss.Load().(map[string][]proto.DB)
	groups := make([]string, 0, len(dss))
	for k := range dss {
		groups = append(groups, k)
	}
	sort.Strings(groups)
	return groups
}

func (ns *Namespace) DB0(ctx context.Context) proto.DB {
	groups := ns.DBGroups()
	if len(groups) < 1 {
		return nil
	}
	return ns.DB(ctx, groups[0])
}

// DB returns a DB, returns nil if nothing selected.
func (ns *Namespace) DB(ctx context.Context, group string) proto.DB {
	// use weight manager to select datasource
	dss := ns.dss.Load().(map[string][]proto.DB)
	exist, ok := dss[group]
	if !ok {
		return nil
	}

	if rcontext.IsMaster(ctx) {
		// TODO: select master
		_ = "todo: select master"
	} else if rcontext.IsSlave(ctx) {
		// TODO: select slave
		_ = "todo: select slave"
	} else {
		// TODO: select by weight
		_ = "todo: select by weight"
	}

	return exist[rand2.Intn(len(exist))]
}

// Optimizer returns the optimizer.
func (ns *Namespace) Optimizer() proto.Optimizer {
	return ns.optimizer
}

// Rule returns the sharding rule.
func (ns *Namespace) Rule() *rule.Rule {
	ru, ok := ns.rule.Load().(*rule.Rule)
	if !ok {
		return nil
	}
	return ru
}

// EnqueueCommand enqueues the next command, it will be executed async.
func (ns *Namespace) EnqueueCommand(cmd Command) error {
	if ns.closed.Load() {
		return io.EOF
	}
	ns.cmds <- cmd
	return nil
}

// Close closes namespace.
func (ns *Namespace) Close() error {
	if !ns.closed.CAS(false, true) {
		return nil
	}

	close(ns.cmds)

	<-ns.done

	ns.Lock()
	defer ns.Unlock()

	for group, dbs := range ns.dss.Load().(map[string][]proto.DB) {
		for _, db := range dbs {
			if err := db.Close(); err != nil {
				log.Errorf("[%s] close DB %s.%s failed: %v", ns.name, group, db.ID(), err)
			}
		}
	}

	log.Infof("[%s] close namespace successfully", ns.name)

	return nil
}

func (ns *Namespace) loopCmds() {
	defer close(ns.done)
	for cmd := range ns.cmds {
		cmd(ns)
	}
}
