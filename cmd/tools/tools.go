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
	"os"
)

import (
	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/cmd/cmds"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
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
	boot.RunImport(importConfPath, configPath)
}
