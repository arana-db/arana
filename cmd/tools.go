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

package main

import (
	"context"
)

import (
	"github.com/spf13/cobra"
)

import (
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/util/log"
)

var ()

var (
	confImportCommand = &cobra.Command{
		Use:   "import",
		Short: "import arana config",
		Run: func(*cobra.Command, []string) {
			provider := boot.NewProvider(configPath)
			if err := provider.Init(context.Background()); err != nil {
				log.Fatal("init failed: %v", err)
				return
			}

			c := provider.GetConfigCenter()
			if err := c.Persist(); err != nil {
				log.Fatal("persist config to config.store failed: %v", err)
				return
			}
		},
	}
)
