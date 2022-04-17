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
	"os"
)

import (
	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/pkg/constants"
)

var (
	Version = "0.1.0"

	bootstrapConfigPath string
	importBootConfPath  string
)

var (
	rootCommand = &cobra.Command{
		Use:     "arana",
		Short:   "arana is a db proxy server",
		Version: Version,
	}
)

// init Init startCmd
func init() {
	startCommand.
		PersistentFlags().
		StringVarP(&bootstrapConfigPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvAranaConfig), "bootstrap configuration file path")

	confImportCommand.
		PersistentFlags().
		StringVarP(&importBootConfPath, constants.ConfigPathKey, "c", os.Getenv(constants.EnvAranaConfig), "bootstrap configuration file path")
	confImportCommand.
		PersistentFlags().
		StringVarP(&sourceConfigPath, constants.ImportConfigPathKey, "s", "", "import configuration file path")

	rootCommand.AddCommand(startCommand)
	rootCommand.AddCommand(confImportCommand)
}

// Execute Execute command line analysis
func Execute() {
	rootCommand.Execute()
}
