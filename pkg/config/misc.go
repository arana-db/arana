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

package config

import (
	"os"
	"path/filepath"
)

import (
	"github.com/go-playground/validator/v10"

	"github.com/pkg/errors"

	"gopkg.in/yaml.v3"
)

import (
	"github.com/arana-db/arana/pkg/util/file"
	"github.com/arana-db/arana/pkg/util/log"
)

var BootOpts *BootOptions

// LoadBootOptions loads BootOptions from specified file path.
func LoadBootOptions(path string) (*BootOptions, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		err = errors.Wrap(err, "failed to load config")
		return nil, err
	}

	if !file.IsYaml(path) {
		err = errors.Errorf("invalid config file format: %s", filepath.Ext(path))
		return nil, err
	}

	var cfg BootOptions
	cfg.Logging = log.DefaultConfig()

	if err = yaml.Unmarshal(content, &cfg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal config")
	}

	if err = validator.New().Struct(&cfg); err != nil {
		return nil, errors.Wrap(err, "failed to validate boot config")
	}

	BootOpts = &cfg
	log.Init(cfg.Logging)
	return &cfg, nil
}

// LoadTenantOperatorFromPath loads tenant operator from specified file path.
func LoadTenantOperatorFromPath(path string) (TenantOperator, error) {
	cfg, err := LoadBootOptions(path)
	if err != nil {
		return nil, err
	}
	if err = Init(*cfg.Config, cfg.Spec.APIVersion); err != nil {
		return nil, err
	}

	return NewTenantOperator(GetStoreOperate())
}

// LoadTenantOperator loads tenant operator from boot option.
func LoadTenantOperator(cfg *BootOptions) (TenantOperator, error) {
	if err := Init(*cfg.Config, cfg.Spec.APIVersion); err != nil {
		return nil, err
	}

	return NewTenantOperator(GetStoreOperate())
}
