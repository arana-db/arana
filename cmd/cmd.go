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
	"context"
	"database/sql"
	"encoding/json"
	"os"
)

import (
	_ "github.com/go-sql-driver/mysql" // register mysql

	"github.com/spf13/cobra"
)

import (
	"github.com/dubbogo/kylin/pkg/config"
	"github.com/dubbogo/kylin/pkg/constants"
	"github.com/dubbogo/kylin/pkg/executor"
	"github.com/dubbogo/kylin/pkg/mysql"
	"github.com/dubbogo/kylin/pkg/resource"
	"github.com/dubbogo/kylin/pkg/server"
	"github.com/dubbogo/kylin/pkg/util/log"
	"github.com/dubbogo/kylin/third_party/pools"
)

var (
	Version = "0.1.0"

	configPath string
)

var (
	rootCommand = &cobra.Command{
		Use:     "kylin",
		Short:   "kylin is a db proxy server",
		Version: Version,
	}

	startCommand = &cobra.Command{
		Use:   "start",
		Short: "start kylin",

		Run: func(cmd *cobra.Command, args []string) {
			conf := config.Load(configPath)
			listener, err := mysql.NewListener(conf.Listeners[0])
			if err != nil {
				panic(err)
			}
			exec := executor.NewRedirectExecutor()
			listener.SetExecutor(exec)

			resource.InitDataSourceManager(conf.DataSources, func(config json.RawMessage) pools.Factory {
				return func(context context.Context) (pools.Resource, error) {
					v := &struct {
						DSN string `json:"dsn"`
					}{}
					if err := json.Unmarshal(config, v); err != nil {
						log.Errorf("unmarshal mysql Listener config failed, %s", err)
						return nil, err
					}
					db, err := sql.Open("mysql", v.DSN)
					return db, err
				}
			})
			kylin := server.NewServer()
			kylin.AddListener(listener)
			kylin.Start()
		},
	}
)

// init Init startCmd
func init() {
	startCommand.PersistentFlags().StringVarP(&configPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvKylinConfig), "Load configuration from `FILE`")
	rootCommand.AddCommand(startCommand)
}

func main() {
	rootCommand.Execute()
}
