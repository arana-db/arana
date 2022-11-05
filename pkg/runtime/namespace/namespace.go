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
	"context"
	"io"
	"sort"
	"sync"
	"time"
)

import (
	"github.com/pkg/errors"

	"go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/proto"
	"github.com/arana-db/arana/pkg/proto/rule"
	rcontext "github.com/arana-db/arana/pkg/runtime/context"
	"github.com/arana-db/arana/pkg/selector"
	"github.com/arana-db/arana/pkg/util/log"
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

		rule atomic.Value // *rule.Rule

		// datasource map, eg: employee_0001 -> [mysql-a,mysql-b,mysql-c], ... employee_0007 -> [mysql-x,mysql-y,mysql-z]
		dss atomic.Value // map[string][]proto.DB

		parameters    config.ParametersMap
		slowThreshold time.Duration

		cmds chan Command  // command queue
		done chan struct{} // done notify

		slowLog log.Logger
	}

	// Command represents the command to control Namespace.
	Command func(ns *Namespace) error
)

// New creates a Namespace.
func New(name string, commands ...Command) (*Namespace, error) {
	ns := &Namespace{
		name: name,
		cmds: make(chan Command, 1),
		done: make(chan struct{}),
	}
	ns.dss.Store(make(map[string][]proto.DB)) // init empty map
	ns.rule.Store(&rule.Rule{})               // init empty rule

	for _, cmd := range commands {
		if err := cmd(ns); err != nil {
			return nil, err
		}
	}

	go ns.loopCmds()

	return ns, nil
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

func (ns *Namespace) DBs(group string) []proto.DB {
	dss := ns.dss.Load().(map[string][]proto.DB)
	exist, ok := dss[group]
	if !ok {
		return nil
	}

	ret := make([]proto.DB, len(exist))
	copy(ret, exist)
	return ret
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
	var (
		target = 0
		wrList = make([]int, 0, len(exist))
	)

	// select by weight
	if rcontext.IsRead(ctx) {
		for _, db := range exist {
			wrList = append(wrList, int(db.Weight().R))
		}
	} else if rcontext.IsWrite(ctx) {
		for _, db := range exist {
			wrList = append(wrList, int(db.Weight().W))
		}
	}
	if len(wrList) != 0 {
		target = selector.NewWeightRandomSelector(wrList).GetDataSourceNo()
	}

	return exist[target]
}

// DBMaster returns a master DB, returns nil if nothing selected.
func (ns *Namespace) DBMaster(_ context.Context, group string) proto.DB {
	// use weight manager to select datasource
	dss := ns.dss.Load().(map[string][]proto.DB)
	exist, ok := dss[group]
	if !ok {
		return nil
	}
	// master weight w>0 && r>0
	for _, db := range exist {
		if db.Weight().W > 0 && db.Weight().R > 0 {
			return db
		}
	}
	return nil
}

// DBSlave returns a slave DB, returns nil if nothing selected.
func (ns *Namespace) DBSlave(_ context.Context, group string) proto.DB {
	// use weight manager to select datasource
	dss := ns.dss.Load().(map[string][]proto.DB)
	exist, ok := dss[group]
	if !ok {
		return nil
	}
	var (
		target     = 0
		wrList     = make([]int, 0, len(exist))
		readDBList = make([]proto.DB, 0, len(exist))
	)
	// slave weight w==0 && r>=0
	for _, db := range exist {
		if db.Weight().W != 0 {
			continue
		}
		// r==0 has high priority
		if db.Weight().R == 0 {
			return db
		}
		if db.Weight().R > 0 {
			wrList = append(wrList, int(db.Weight().R))
			readDBList = append(readDBList, db)
		}
	}
	if len(wrList) != 0 {
		target = selector.NewWeightRandomSelector(wrList).GetDataSourceNo()
	}
	return readDBList[target]
}

// Rule returns the sharding rule.
func (ns *Namespace) Rule() *rule.Rule {
	ru, ok := ns.rule.Load().(*rule.Rule)
	if !ok {
		return nil
	}
	return ru
}

func (ns *Namespace) Parameters() config.ParametersMap {
	return ns.parameters
}

func (ns *Namespace) SlowThreshold() time.Duration {
	return ns.slowThreshold
}

func (ns *Namespace) SlowLogger() log.Logger {
	return ns.slowLog
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
		_ = cmd(ns)
	}
}
