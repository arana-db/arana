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
	"fmt"
	"os"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadBootOptions(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "example.*.yaml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	text := `
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
logging:
  level: INFO
  path: /var/log/arana
  max_size: 128m
  max_backups: 3
  max_age: 7
  compress: true
  console: true
`
	_, err = tmpfile.WriteString(text)
	require.NoError(t, err)
	err = tmpfile.Close()
	require.NoError(t, err)

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
	assert.Equal(t, "INFO", cfg.Logging.Level)
	assert.Equal(t, "/var/log/arana", cfg.Logging.Path)
	assert.Equal(t, "128m", cfg.Logging.MaxSize)
	assert.Equal(t, 3, cfg.Logging.MaxBackups)
	assert.Equal(t, 7, cfg.Logging.MaxAge)
	assert.True(t, cfg.Logging.Compress)
	assert.False(t, cfg.Logging.SqlLogEnabled)
}

func TestLoadBootOptions_Error(t *testing.T) {
	cfg, err := LoadBootOptions("")
	assert.Error(t, err)
	assert.Nil(t, cfg)

	text0 := `
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
logging:
  level: INFO
  path: /var/log/arana
  max_size: 128m
  max_backups: 3
  max_age: 7
  compress: true
  console: true
`
	tmpfile0, err := os.CreateTemp("", "example.*.xml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile0.Name())
	_, err = tmpfile0.WriteString(text0)
	require.NoError(t, err)
	err = tmpfile0.Close()
	require.NoError(t, err)

	cfg0, err := LoadBootOptions(tmpfile0.Name())
	assert.Error(t, err)
	assert.Nil(t, cfg0)

	text1 := `
<listeners>
    <protocol_type>http</protocol_type>
    <server_version>1.0</server_version>
</listeners>
`
	tmpfile1, err := os.CreateTemp("", "example.*.yaml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile1.Name())
	_, err = tmpfile1.WriteString(text1)
	require.NoError(t, err)
	err = tmpfile1.Close()
	require.NoError(t, err)

	cfg1, err := LoadBootOptions(tmpfile1.Name())
	assert.Error(t, err)
	assert.Nil(t, cfg1)

	text2 := `
listeners_error:
- protocol_type: "http"
  server_version: "1.0"
registry:
  enable: true
  name: "registryName"
  root_path: "/root/path"
trace:
  type: "jaeger"
  address: "http://localhost:14268/api/traces"
supervisor_error:
  username: "admin"
  password: "password"
logging:
  level: INFO
  path: /var/log/arana
  max_size: 128m
  max_backups: 3
  max_age: 7
  compress: true
  console: true
`
	tmpfile2, err := os.CreateTemp("", "example.*.yaml")
	if err != nil {
		t.Fatal(err)
	}

	defer os.Remove(tmpfile2.Name())
	_, err = tmpfile2.WriteString(text2)
	require.NoError(t, err)
	err = tmpfile2.Close()
	require.NoError(t, err)

	cfg2, err := LoadBootOptions(tmpfile2.Name())
	assert.Error(t, err)
	assert.Nil(t, cfg2)
}

func TestLoadTenantOperator(t *testing.T) {
	root, config := bootstrapPath(t)
	defer func() {
		_ = os.Remove(root)
		_ = os.Remove(config)
	}()
	_, err := LoadBootOptions(root)
	assert.NoError(t, err)
}

func TestLoadTenantOperatorFromPath(t *testing.T) {
	_, err := LoadTenantOperatorFromPath("")
	assert.Error(t, err)
}

func bootstrapPath(t *testing.T) (string, string) {
	create := func(config string) string {
		tmp, err := os.CreateTemp("", "arana-fake.*.yaml")
		assert.NoError(t, err)
		_, err = tmp.WriteString(config)
		assert.NoError(t, err)
		return tmp.Name()
	}

	config := `kind: ConfigMap
apiVersion: "1.0"
metadata:
  name: arana-config
data:
  tenants:
    - name: arana
      sys_db:
        host: arana-mysql
        port: 3306
        username: root
        password: "123456"
        database: __arana_sys
        weight: r10w10
        parameters:
      users:
        - username: arana
          password: "123456"
      clusters:
        - name: employees
          type: mysql
          sql_max_limit: -1
          tenant: arana
          parameters:
            slow_threshold: 1s
            max_allowed_packet: 256M
          groups:
            - name: employees_0000
              nodes:
                - node0
      sharding_rule:
        tables:
          - name: employees.student
            sequence:
              type: snowflake
              option:
            db_rules:
              - columns:
                  - name: uid
                expr: parseInt($0 % 32 / 8)
            tbl_rules:
              - columns:
                  - name: uid
                expr: $0 % 32
            topology:
              db_pattern: employees_${0000..0003}
              tbl_pattern: student_${0000..0031}
            attributes:
              allow_full_scan: true
              sqlMaxLimit: -1
          - name: employees.friendship
            sequence:
              type: snowflake
              option:
            db_rules:
              - columns:
                  - name: uid
                  - name: friend_id
                expr: parseInt(($0*31+$1) % 32 / 8)
            tbl_rules:
              - columns:
                  - name: uid
                  - name: friend_id
                expr: ($0*31+$1) % 32
            topology:
              db_pattern: employees_${0000..0003}
              tbl_pattern: friendship_${0000..0031}
            attributes:
              allow_full_scan: true
              sqlMaxLimit: -1
      nodes:
        node0:
          name: node0
          host: arana-mysql
          port: 3306
          username: root
          password: "123456"
          database: employees_0000
          weight: r10w10
          parameters:`
	configPath := create(config)
	assert.NoError(t, os.WriteFile(configPath, []byte(config), 0644))

	bootstrap := `listeners:
- protocol_type: "http"
  server_version: "1.0"
config:
  name: file
  options:
    path: %s
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
logging:
  level: INFO
  path: /Users/baerwang/project/arana-db/arana
  max_size: 128m
  max_backups: 3
  max_age: 7
  compress: true
  console: true`
	return create(fmt.Sprintf(bootstrap, configPath)), configPath
}
