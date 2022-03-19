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
	"context"
	"os"
	"os/signal"
	"syscall"
)

import (
	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/executor"
	"github.com/arana-db/arana/pkg/filters"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/server"
	"github.com/arana-db/arana/pkg/util/log"
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
			provider := boot.NewFileProvider(configPath)
			if err := boot.Boot(context.Background(), provider); err != nil {
				log.Fatal("start failed: %v", err)
				return
			}

			filters, err := provider.ListFilters(context.Background())
			if err != nil {
				log.Fatal("start failed: %v", err)
				return
			}

			for _, filterConf := range filters {
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

			propeller := server.NewServer()

			listenersConf, err := provider.ListListeners(context.Background())
			if err != nil {
				log.Fatal("start failed: %v", err)
				return
			}

			for _, listenerConf := range listenersConf {
				listener, err := mysql.NewListener(listenerConf)
				if err != nil {
					log.Fatalf("create listener failed: %v", err)
					return
				}
				executor := executor.NewRedirectExecutor()
				listener.SetExecutor(executor)
				propeller.AddListener(listener)
			}
			propeller.Start()

			ctx, cancel := context.WithCancel(context.Background())
			c := make(chan os.Signal, 2)
			signal.Notify(c, os.Interrupt, syscall.SIGTERM)
			go func() {
				<-c
				cancel()
				<-c
				os.Exit(1) // second signal. Exit directly.
			}()
			select {
			case <-ctx.Done():
				return
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
