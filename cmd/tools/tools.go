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

package tools

import (
	"context"
	"os"
)

import (
	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/cmd/cmds"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/constants"
	"github.com/arana-db/arana/pkg/util/log"
)

var (
	sourceConfigPath   string
	importBootConfPath string
)

// init Init startCmd
func init() {
	cmd := &cobra.Command{
		Use:     "import",
		Short:   "import arana config",
		Example: "./arana import -c ../docker/conf/bootstrap.yaml -s ../docker/conf/config.yaml",
		Run:     run,
	}

	cmd.PersistentFlags().
		StringVarP(&importBootConfPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvBootstrapPath), "bootstrap configuration file path")
	cmd.PersistentFlags().
		StringVarP(&sourceConfigPath, constants.ImportConfigPathKey, "s", "", "import configuration file path")

	cmds.Handle(func(root *cobra.Command) {
		root.AddCommand(cmd)
	})
}

func run(_ *cobra.Command, _ []string) {
	Run(importBootConfPath, sourceConfigPath)
}

func Run(importConfPath, configPath string) {
	bootCfg, err := boot.LoadBootOptions(importConfPath)
	if err != nil {
		log.Fatalf("load bootstrap config failed: %+v", err)
	}

	if err := config.Init(*bootCfg.Config, bootCfg.APIVersion); err != nil {
		log.Fatal()
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal("load config from %s failed: %+v", configPath, err)
		return
	}

	tenantOp, err := config.NewTenantOperator(config.GetStoreOperate())
	if err != nil {
		log.Fatal("build tenant operator failed: %+v", configPath, err)
		return
	}

	for i := range cfg.Data.Tenants {
		if err := tenantOp.CreateTenant(cfg.Data.Tenants[i].Name); err != nil {
			log.Fatal("create tenant failed: %+v", configPath, err)
			return
		}
	}

	for i := range cfg.Data.Tenants {

		tenant := cfg.Data.Tenants[i]

		tenant.APIVersion = cfg.APIVersion
		tenant.Metadata = cfg.Metadata

		op := config.NewCenter(tenant.Name, config.GetStoreOperate())

		if err := op.Import(context.Background(), tenant); err != nil {
			log.Fatalf("persist config to config.store failed: %+v", err)
			return
		}
	}

	log.Infof("finish import config into config_center")
}
