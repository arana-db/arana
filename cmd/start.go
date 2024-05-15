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

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/executor"
	"github.com/arana-db/arana/pkg/mysql"
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/server"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/spf13/cobra"
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

const _keyBootstrap = "config"

func init() {
	cmd := &cobra.Command{
		Use:     "start",
		Short:   "start arana",
		Example: "arana start -c bootstrap.yaml",
		Run:     runStart,
	}
	cmd.PersistentFlags().
		StringP(_keyBootstrap, "c", os.Getenv(constants.EnvBootstrapPath), "bootstrap configuration file path")

	RootCommand.AddCommand(cmd)
}

func runStart(cmd *cobra.Command, _ []string) {
	bootstrapConfigPath, _ := cmd.PersistentFlags().GetString(_keyBootstrap)
	Start(bootstrapConfigPath)
}

func Start(bootstrapConfigPath string) {
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

	// print slogan
	fmt.Printf("\033[92m%s\033[0m\n", slogan) // 92m: light green

	discovery := boot.NewDiscovery(bootstrapConfigPath)

	if err := boot.Boot(context.Background(), discovery); err != nil {
		log.Fatalf("start failed: %v", err)
		return
	}

	propeller := server.NewServer()

	listenersConf := discovery.ListListeners(context.Background())
	for _, listenerConf := range listenersConf {
		listener, err := mysql.NewListener(listenerConf)
		if err != nil {
			log.Fatalf("create listener failed: %v", err)
			return
		}
		listener.SetExecutor(executor.NewRedirectExecutor())
		propeller.AddListener(listener)
	}

	// init service registry
	registryConf := discovery.GetServiceRegistry(context.Background())
	if registryConf != nil && registryConf.Enable {
		serviceRegistry, err := registry.InitRegistry(registryConf)
		if err != nil {
			log.Errorf("create service registry failed: %v", err)
			return
		}

		if err = registry.DoRegistry(context.Background(), serviceRegistry, "service", listenersConf); err != nil {
			log.Errorf("do service register failed: %v", err)
			return
		}
	}

	if err := discovery.InitTrace(context.Background()); err != nil {
		log.Warnf("init trace provider failed: %v", err)
	}

	if err := discovery.InitPrometheus(context.Background()); err != nil {
		log.Warnf("init prometheus provider failed: %v", err)
	}

	if err := discovery.InitSupervisor(context.Background()); err != nil {
		log.Warnf("init supervisor failed: %v", err)
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

	<-ctx.Done()
}
