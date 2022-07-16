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

package constants

import (
	"os"
	"path/filepath"
)

// Environments
const (
	EnvBootstrapPath      = "ARANA_BOOTSTRAP_PATH"      // bootstrap file path, eg: /etc/arana/bootstrap.yaml
	EnvConfigPath         = "ARANA_CONFIG_PATH"         // config file path, eg: /etc/arana/config.yaml
	EnvDevelopEnvironment = "ARANA_DEVELOP_ENVIRONMENT" // config dev environment
)

// GetConfigSearchPathList returns the default search path list of configuration.
func GetConfigSearchPathList() []string {
	var dirs []string
	dirs = append(dirs, ".", "./conf")
	if home, err := os.UserHomeDir(); err == nil {
		dirs = append(dirs, filepath.Join(home, ".arana"))
	}
	dirs = append(dirs, "/etc/arana")
	return dirs
}
