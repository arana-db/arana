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
	"errors"
	"fmt"
	"io"
)

type (
	// ProtocolType protocol type enum
	ProtocolType int32
)

const (
	DefaultConfigPath                   = "/arana-db/config"
	DefaultConfigMetadataPath           = "/arana-db/config/metadata"
	DefaultConfigDataListenersPath      = "/arana-db/config/data/listeners"
	DefaultConfigDataFiltersPath        = "/arana-db/config/data/filters"
	DefaultConfigDataSourceClustersPath = "/arana-db/config/data/dataSourceClusters"
	DefaultConfigDataShardingRulePath   = "/arana-db/config/data/shardingRule"
	DefaultConfigDataTenantsPath        = "/arana-db/config/data/tenants"
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
	slots        map[string]StoreOperate = make(map[string]StoreOperate)
	storeOperate StoreOperate
)

func GetStoreOperate() (StoreOperate, error) {
	if storeOperate != nil {
		return storeOperate, nil
	}

	return nil, errors.New("StoreOperate not init")
}

func Init(name string, options map[string]interface{}) error {

	s, exist := slots[name]
	if !exist {
		return fmt.Errorf("StoreOperate solt=[%s] not exist", name)
	}

	storeOperate = s

	return storeOperate.Init(options)
}

//Register 注册一个存储插件
func Register(s StoreOperate) {
	if _, ok := slots[s.Name()]; ok {
		panic(fmt.Errorf("StoreOperate=[%s] already exist", s.Name()))
	}

	slots[s.Name()] = s
}

//StoreOperate 配置存储相关插件
type StoreOperate interface {
	io.Closer

	//Init 插件初始化
	Init(options map[string]interface{}) error

	//Save 保持某一个配置数据
	Save(key string, val []byte) error

	//Get 获取某一个配置
	Get(key string) ([]byte, error)

	//Watch 监听
	Watch(key string) (<-chan []byte, error)

	//Name 插件名称
	Name() string
}
