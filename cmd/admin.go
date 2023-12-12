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
	"path/filepath"
)

import (
	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	_ "github.com/arana-db/arana/pkg/admin/router"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/config"
	_ "github.com/arana-db/arana/pkg/config/etcd"
	_ "github.com/arana-db/arana/pkg/config/file"
	_ "github.com/arana-db/arana/pkg/config/nacos"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/registry"
	"github.com/arana-db/arana/pkg/security"
	"github.com/arana-db/arana/pkg/util/log"
)

const (
	_keyPort     = "port"
	_defaultPort = 8080
)

func init() {
	cmd := &cobra.Command{
		Use:     "admin",
		Short:   "admin",
		Example: "arana admin -c bootstrap.yaml -p 8080",
		RunE:    runAdmin,
	}
	cmd.PersistentFlags().
		StringP(constants.ConfigPathKey, "c", os.Getenv(constants.EnvBootstrapPath), "bootstrap configuration file path")
	cmd.PersistentFlags().
		Uint16P(_keyPort, "p", _defaultPort, "listen port")

	RootCommand.AddCommand(cmd)
}

func runAdmin(cmd *cobra.Command, args []string) error {
	_ = args
	bootstrapPath, _ := cmd.PersistentFlags().GetString(constants.ConfigPathKey)
	port, _ := cmd.PersistentFlags().GetUint16("port")
	if len(bootstrapPath) < 1 {
		// search bootstrap yaml
		for _, path := range constants.GetConfigSearchPathList() {
			bootstrapPath = filepath.Join(path, "bootstrap.yaml")
			if _, err := os.Stat(bootstrapPath); err == nil {
				break
			}
			bootstrapPath = filepath.Join(path, "bootstrap.yml")
			if _, err := os.Stat(bootstrapPath); err == nil {
				break
			}
		}
	}

	bootOptions, err := config.LoadBootOptions(bootstrapPath)
	if err != nil {
		return err
	}
	security.DefaultTenantManager().SetSupervisor(bootOptions.Supervisor)

	op, err := config.LoadTenantOperator(bootOptions)
	if err != nil {
		log.Fatalf("start admin api server failed: %v", err)
		return err
	}
	discovery := boot.NewDiscovery(bootstrapPath)
	if err = boot.Boot(context.Background(), discovery); err != nil {
		log.Fatalf("start failed: %v", err)
		return err
	}

	registryConf := discovery.GetServiceRegistry(context.Background())
	serviceDiscovery, err := registry.InitDiscovery(registryConf.Name, registryConf.Options)
	if err != nil {
		log.Fatalf("init service discovery failed: %v", err)
		return err
	}

	adminServer := admin.New(op, serviceDiscovery)
	return adminServer.Listen(fmt.Sprintf(":%d", port))
}
