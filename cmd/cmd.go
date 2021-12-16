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

package main

import (
	"encoding/json"
	"os"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

import (
	"github.com/dubbogo/arana/pkg/config"
	"github.com/dubbogo/arana/pkg/constants"
	"github.com/dubbogo/arana/pkg/executor"
	filter "github.com/dubbogo/arana/pkg/filters"
	"github.com/dubbogo/arana/pkg/mysql"
	"github.com/dubbogo/arana/pkg/proto"
	"github.com/dubbogo/arana/pkg/resource"
	"github.com/dubbogo/arana/pkg/server"
	"github.com/dubbogo/arana/third_party/pools"
)

var (
	Version = "0.1.0"

	configPath string
)

var (
	rootCommand = &cobra.Command{
		Use:     "arana",
		Short:   "arana is a db proxy server",
		Version: Version,
	}

	startCommand = &cobra.Command{
		Use:   "start",
		Short: "start arana",

		Run: func(cmd *cobra.Command, args []string) {
			conf := config.Load(configPath)

			for _, filterConf := range conf.Filters {
				factory := filter.GetFilterFactory(filterConf.Name)
				if factory == nil {
					panic(errors.Errorf("there is no filter factory for filter: %s", filterConf.Name))
				}
				f, err := factory.NewFilter(filterConf.Config)
				if err != nil {
					panic(errors.WithMessagef(err, "failed to create filter: %s", filterConf.Name))
				}
				filter.RegisterFilter(f.GetName(), f)
			}

			executors := make(map[string]proto.Executor)
			for _, executorConf := range conf.Executors {
				executor := executor.NewRedirectExecutor(executorConf)

				for i := 0; i < len(executorConf.Filters); i++ {
					filterName := executorConf.Filters[i]
					f := filter.GetFilter(filterName)
					if f != nil {
						preFilter, ok := f.(proto.PreFilter)
						if ok {
							executor.AddPreFilter(preFilter)
						}
						postFilter, ok := f.(proto.PostFilter)
						if ok {
							executor.AddPostFilter(postFilter)
						}
					}
				}
				executors[executorConf.Name] = executor
			}

			resource.InitDataSourceManager(conf.DataSources, func(config json.RawMessage) pools.Factory {
				collector, err := mysql.NewConnector(config)
				if err != nil {
					panic(err)
				}
				return collector.NewBackendConnection
			})
			propeller := server.NewServer()

			for _, listenerConf := range conf.Listeners {
				listener, err := mysql.NewListener(listenerConf)
				if err != nil {
					panic(err)
				}
				executor := executors[listenerConf.Executor]
				if executor == nil {
					panic(errors.Errorf("executor: %s is not exists for listener: %s:%d",
						listenerConf.Executor,
						listenerConf.SocketAddress.Address,
						listenerConf.SocketAddress.Port))
				}
				listener.SetExecutor(executors[conf.Listeners[0].Executor])
				propeller.AddListener(listener)
				propeller.Start()
			}
		},
	}
)

// init Init startCmd
func init() {
	startCommand.PersistentFlags().StringVarP(&configPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvAranaConfig), "Load configuration from `FILE`")
	rootCommand.AddCommand(startCommand)
}

func main() {
	rootCommand.Execute()
}
