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

package admin

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
)

import (
	"github.com/gin-gonic/gin"

	perrors "github.com/pkg/errors"

	uatomic "go.uber.org/atomic"
)

import (
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
)

const K = "ARANA_ADMIN_SERVICE"

var NotFoundError = errors.New("resource not found")

var _hooks []Hook

type Hook func(router gin.IRoutes)

func Register(hook Hook) {
	_hooks = append(_hooks, hook)
}

func init() {
	switch strings.ToLower(os.Getenv(constants.EnvDevelopEnvironment)) {
	case "1", "true", "yes", "on":
		gin.SetMode(gin.DebugMode)
	default:
		gin.SetMode(gin.ReleaseMode)
	}
}

type Service = boot.ConfigProvider

type Server struct {
	l       net.Listener
	engine  *gin.Engine
	service Service
	started uatomic.Bool
}

func New(service Service) *Server {
	return &Server{
		service: service,
		engine:  gin.New(),
	}
}

func (srv *Server) Close() error {
	if srv.l != nil {
		return srv.l.Close()
	}
	return nil
}

func (srv *Server) Listen(addr string) error {
	if !srv.started.CAS(false, true) {
		return io.EOF
	}

	var (
		c   net.ListenConfig
		err error
	)

	srv.engine.Use(func(c *gin.Context) {
		c.Set(K, srv.service)
		c.Next()
	})
	srv.engine.Use(gin.Logger())
	srv.engine.Use(gin.Recovery())
	srv.engine.Use(
		ErrorHandler(
			Map(NotFoundError).
				ToResponse(func(c *gin.Context, err error) {
					c.Status(http.StatusNotFound)
					_, _ = c.Writer.WriteString(err.Error())
				}),
		))

	for _, hook := range _hooks {
		hook(srv.engine)
	}

	if srv.l, err = c.Listen(context.Background(), "tcp", addr); err != nil {
		return perrors.WithStack(err)
	}
	return srv.engine.RunListener(srv.l)
}

// GetService returns Service from gin context.
func GetService(c *gin.Context) Service {
	v, _ := c.Get(K)
	return v.(Service)
}
