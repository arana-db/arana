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

package test

import (
	"context"
	"path"
)

import (
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

import (
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/testdata"
)

type MySQLContainer struct {
	testcontainers.Container
	Host string
	Port int
}

type MySQLContainerTester struct {
	Username string `validate:"required" yaml:"username" json:"username"`
	Password string `validate:"required" yaml:"password" json:"password"`
	Database string `validate:"required" yaml:"database" json:"database"`

	ScriptPath string
}

func (tester MySQLContainerTester) SetupMySQLContainer(ctx context.Context) (*MySQLContainer, error) {
	log.Info("Setup MySQL Container")
	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0",
		ExposedPorts: []string{"3306/tcp", "33060/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": tester.Password,
			"MYSQL_DATABASE":      tester.Database,
		},
		BindMounts: map[string]string{
			"/docker-entrypoint-initdb.d/0.sql": testdata.Path(path.Join(tester.ScriptPath, "init.sql")),
			"/docker-entrypoint-initdb.d/1.sql": testdata.Path(path.Join(tester.ScriptPath, "sharding.sql")),
			"/docker-entrypoint-initdb.d/2.sql": testdata.Path(path.Join(tester.ScriptPath, "sequence.sql")),
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server - GPL"),
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	mp, err := c.MappedPort(ctx, "3306")
	if err != nil {
		return nil, err
	}
	hostIP, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}

	if hostIP == "localhost" {
		hostIP = "127.0.0.1"
	}

	return &MySQLContainer{
		Container: c,
		Host:      hostIP,
		Port:      mp.Int(),
	}, nil
}
