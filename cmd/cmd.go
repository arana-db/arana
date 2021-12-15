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
	"github.com/dubbogo/arana/pkg/proto"
	"os"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/spf13/cobra"
)

import (
	"github.com/dubbogo/arana/pkg/config"
	"github.com/dubbogo/arana/pkg/constants"
	"github.com/dubbogo/arana/pkg/executor"
	"github.com/dubbogo/arana/pkg/mysql"
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

			executors := make(map[string]proto.Executor)
			for _, executorConf := range conf.Executors {
				executor := executor.NewRedirectExecutor(executorConf)
				executors[executorConf.Name] = executor
			}

			listener, err := mysql.NewListener(conf.Listeners[0])
			if err != nil {
				panic(err)
			}
			listener.SetExecutor(executors[conf.Listeners[0].Executor])

			resource.InitDataSourceManager(conf.DataSources, func(config json.RawMessage) pools.Factory {
				collector, err := mysql.NewConnector(config)
				if err != nil {
					panic(err)
				}
				return collector.NewBackendConnection
			})
			arana := server.NewServer()
			arana.AddListener(listener)
			arana.Start()
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
