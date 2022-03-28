//
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

package config

import (
	"fmt"
	"go.uber.org/atomic"
)

type (
	// ProtocolType protocol type enum
	ProtocolType int32
)

const (
	DefaultConfigPath                   = "/arana-db/config"
	DefaultConfigDataListenersPath      = "/arana-db/config/data/listeners"
	DefaultConfigDataExecutorsPath      = "/arana-db/config/data/executors"
	DefaultConfigDataSourceClustersPath = "/arana-db/config/data/dataSourceClusters"
	DefaultConfigDataShardingRulePath   = "/arana-db/config/data/shardingRule"
)

const (
	Http ProtocolType = iota
	Mysql
)

const (
	_ DataSourceType = iota
	DBMysql
	DBPostgreSql
)

var (
	slots map[string]StoreOperate
)

func Register(s StoreOperate) {
	if _, ok := slots[s.Name()]; ok {
		panic(fmt.Errorf("StoreOperate=[%s] already exist", s.Name()))
	}

	slots[s.Name()] = s
}

type Observer func()

type StoreOperate interface {
	Save()

	Get()

	Delete()

	Watch(receiver chan struct{})

	Name() string
}

type Changeable interface {
	Name() string
	Sign() string
}

type Center struct {
	confHolder atomic.Value // 里面持有了最新的 *Configuration 对象
	observers  []Observer
}
