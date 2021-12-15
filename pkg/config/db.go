//
// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

import (
	"github.com/pkg/errors"
)

type (
	// DataSourceRole
	DataSourceRole int

	// DataSourceType
	DataSourceType int

	// DataSource
	DataSource struct {
		Role           DataSourceRole  `yaml:"role" json:"role"`
		Type           DataSourceType  `yaml:"type" json:"type"`
		Name           string          `yaml:"name" json:"name"`
		Conf           json.RawMessage `yaml:"conf" json:"conf"`
		Capacity       int             `yaml:"capacity" json:"capacity"`         // connection pool capacity
		MaxCapacity    int             `yaml:"max_capacity" json:"max_capacity"` // max connection pool capacity
		IdleTimeout    time.Duration   `yaml:"idle_timeout" json:"_"`            // close backend direct connection after idle_timeout
		IdleTimeoutStr string          `yaml:"_" json:"idle_timeout"`
	}
)

const (
	Master DataSourceRole = iota
	Slave
	Meta
)

const (
	DBMysql DataSourceType = iota
	DBPostgreSql
)

func (r *DataSourceRole) UnmarshalText(text []byte) error {
	if r == nil {
		return errors.New("can'r unmarshal a nil *DataSourceRole")
	}
	if r.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized protocol type: %q", text)
	}
	return nil
}

func (r *DataSourceRole) unmarshalText(text []byte) bool {
	dataSourceRole := string(text)
	switch dataSourceRole {
	case "master":
		*r = Master
	case "slave":
		*r = Slave
	case "meta":
		*r = Meta
	default:
		return false
	}
	return true
}

func (t *DataSourceType) UnmarshalText(text []byte) error {
	if t == nil {
		return errors.New("can't unmarshal a nil *DataSourceRole")
	}
	if t.unmarshalText(bytes.ToLower(text)) {
		return fmt.Errorf("unrecognized protocol type: %q", text)
	}
	return nil
}

func (t *DataSourceType) unmarshalText(text []byte) bool {
	dataSourceType := string(text)
	switch dataSourceType {
	case "mysql":
		*t = DBMysql
	case "postgresql":
		*t = DBPostgreSql
	default:
		return false
	}
	return true
}
