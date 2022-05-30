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

package start

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

import (
	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/cmd/cmds"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/executor"
	filter "github.com/arana-db/arana/pkg/filters"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/server"
	"github.com/arana-db/arana/pkg/util/log"
)

// slogan is generated from 'figlet -f smslant ARANA'.
const slogan = `
   ___   ___  ___   _  _____ 
  / _ | / _ \/ _ | / |/ / _ |
 / __ |/ , _/ __ |/    / __ |
/_/ |_/_/|_/_/ |_/_/|_/_/ |_|
Arana, A High performance & Powerful DB Mesh sidecar.
_____________________________________________

`

func init() {
	cmd := &cobra.Command{
		Use:     "start",
		Short:   "start arana",
		Example: "arana start -c bootstrap.yaml",
		Run:     run,
	}
	cmd.PersistentFlags().
		StringP(constants.ConfigPathKey, "c", os.Getenv(constants.EnvBootstrapPath), "bootstrap configuration file path")

	cmds.Handle(func(root *cobra.Command) {
		root.AddCommand(cmd)
	})
}

func Run(bootstrapConfigPath string) {
	// print slogan
	fmt.Printf("\033[92m%s\033[0m\n", slogan) // 92m: light green

	provider := boot.NewProvider(bootstrapConfigPath)
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
		listener.SetExecutor(executor.NewRedirectExecutor())
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
}

func run(cmd *cobra.Command, args []string) {
	_ = args
	bootstrapConfigPath, _ := cmd.PersistentFlags().GetString(constants.ConfigPathKey)
	if len(bootstrapConfigPath) < 1 {
		// search bootstrap yaml
		for _, path := range constants.GetConfigSearchPathList() {
			bootstrapConfigPath = filepath.Join(path, "bootstrap.yaml")
			if _, err := os.Stat(bootstrapConfigPath); err == nil {
				break
			}
			bootstrapConfigPath = filepath.Join(path, "bootstrap.yml")
			if _, err := os.Stat(bootstrapConfigPath); err == nil {
				break
			}
		}
	}

	Run(bootstrapConfigPath)
}
