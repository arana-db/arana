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
		Run:     Run,
	}

	cmd.PersistentFlags().
		StringVarP(&importBootConfPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvBootstrapPath), "bootstrap configuration file path")
	cmd.PersistentFlags().
		StringVarP(&sourceConfigPath, constants.ImportConfigPathKey, "s", "", "import configuration file path")

	cmds.Handle(func(root *cobra.Command) {
		root.AddCommand(cmd)
	})
}

func Run(cmd *cobra.Command, args []string) {
	_, _ = cmd, args

	discovery := boot.NewDiscovery(importBootConfPath)
	if err := discovery.Init(context.Background()); err != nil {
		log.Fatal("init failed: %+v", err)
		return
	}

	cfg, err := config.Load(sourceConfigPath)
	if err != nil {
		log.Fatal("load config from %s failed: %+v", sourceConfigPath, err)
		return
	}

	for i := range cfg.Data.Tenants {

		tenant := cfg.Data.Tenants[i]

		tenant.APIVersion = cfg.APIVersion
		tenant.Metadata = cfg.Metadata

		if err := discovery.Import(context.Background(), tenant); err != nil {
			log.Fatal("persist config to config.store failed: %+v", err)
			return
		}
	}
}
