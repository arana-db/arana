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

package boot

import (
	"github.com/arana-db/arana/pkg/config"
)

type (
	BootOptions struct {
		config.Spec `yaml:",inline"`
		Config      *config.Options    `yaml:"config" json:"config"`
		Listeners   []*config.Listener `validate:"required,dive" yaml:"listeners" json:"listeners"`
		Registry    *config.Registry   `yaml:"registry" json:"registry"`
		Trace       *config.Trace      `yaml:"trace" json:"trace"`
	}
)
