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
 *
 */

package config

import (
	"os"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestLoadBootOptions(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "example.*.yaml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	text := []byte(`
listeners:
- protocol_type: "http"
  server_version: "1.0"
registry:
  enable: true
  name: "registryName"
  root_path: "/root/path"
trace:
  type: "jaeger"
  address: "http://localhost:14268/api/traces"
supervisor:
  username: "admin"
  password: "password"
logging_config:
  log_name: "test"
  log_path: "/log/path"
  log_level: 1
  log_max_size: 500
  log_max_backups: 5
  log_max_age: 10
  log_compress: false
  default_log_name: "default"
  tx_log_name: "tx"
  sql_log_enabled: true
  sql_log_name: "sql"
  physical_sql_log_name: "physical_sql"
`)
	if _, err := tmpfile.Write(text); err != nil {
		t.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadBootOptions(tmpfile.Name())
	assert.NoError(t, err)

	assert.Equal(t, "http", cfg.Listeners[0].ProtocolType)
	assert.Equal(t, "1.0", cfg.Listeners[0].ServerVersion)
	assert.True(t, cfg.Registry.Enable)
	assert.Equal(t, "registryName", cfg.Registry.Name)
	assert.Equal(t, "/root/path", cfg.Registry.RootPath)
	assert.Equal(t, "jaeger", cfg.Trace.Type)
	assert.Equal(t, "http://localhost:14268/api/traces", cfg.Trace.Address)
	assert.Equal(t, "admin", cfg.Supervisor.Username)
	assert.Equal(t, "password", cfg.Supervisor.Password)
	assert.Equal(t, "test", cfg.LoggingConfig.LogName)
	assert.Equal(t, "/log/path", cfg.LoggingConfig.LogPath)
	assert.Equal(t, 1, cfg.LoggingConfig.LogLevel)
	assert.Equal(t, 500, cfg.LoggingConfig.LogMaxSize)
	assert.Equal(t, 5, cfg.LoggingConfig.LogMaxBackups)
	assert.Equal(t, 10, cfg.LoggingConfig.LogMaxAge)
	assert.False(t, cfg.LoggingConfig.LogCompress)
	assert.Equal(t, "default", cfg.LoggingConfig.DefaultLogName)
	assert.Equal(t, "tx", cfg.LoggingConfig.TxLogName)
	assert.True(t, cfg.LoggingConfig.SqlLogEnabled)
	assert.Equal(t, "sql", cfg.LoggingConfig.SqlLogName)
	assert.Equal(t, "physical_sql", cfg.LoggingConfig.PhysicalSqlLogName)
}
