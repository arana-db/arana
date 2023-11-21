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
