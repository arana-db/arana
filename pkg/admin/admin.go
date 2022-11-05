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
	"github.com/arana-db/arana/pkg/admin/exception"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/constants"
)

const (
	_envUIPath     = "ARANA_UI"
	_defaultUIPath = "/var/www/arana"
)

const K = "ARANA_ADMIN_SERVICE"

var _hooks []Hook

type Hook func(Router)

type Handler func(*gin.Context) error

type Router interface {
	GET(string, Handler)
	POST(string, Handler)
	DELETE(string, Handler)
	PATCH(string, Handler)
	PUT(string, Handler)
}

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

type Service interface {
	boot.ConfigUpdater
	boot.ConfigProvider
}

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
	srv.engine.Use(CORSMiddleware())

	// Mount APIs
	rg := srv.engine.Group("/api/v1")
	for _, hook := range _hooks {
		hook((*myRouter)(rg))
	}

	// Mount static resources
	uiPath := _defaultUIPath
	if e, ok := os.LookupEnv(_envUIPath); ok {
		uiPath = e
	}
	srv.engine.NoRoute(gin.WrapH(http.FileServer(http.Dir(uiPath))))

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

type myRouter gin.RouterGroup

func (w *myRouter) GET(s string, handler Handler) {
	(*gin.RouterGroup)(w).GET(s, w.wrapper(handler))
}

func (w *myRouter) POST(s string, handler Handler) {
	(*gin.RouterGroup)(w).POST(s, w.wrapper(handler))
}

func (w *myRouter) DELETE(s string, handler Handler) {
	(*gin.RouterGroup)(w).DELETE(s, w.wrapper(handler))
}

func (w *myRouter) PATCH(s string, handler Handler) {
	(*gin.RouterGroup)(w).PATCH(s, w.wrapper(handler))
}

func (w *myRouter) PUT(s string, handler Handler) {
	(*gin.RouterGroup)(w).PUT(s, w.wrapper(handler))
}

func (w *myRouter) wrapper(handler Handler) gin.HandlerFunc {
	return func(c *gin.Context) {
		err := handler(c)
		if err == nil {
			return
		}
		switch e := err.(type) {
		case exception.APIException:
			c.JSON(e.Code.HttpStatus(), e)
		case *exception.APIException:
			c.JSON(e.Code.HttpStatus(), e)
		default:
			ee := exception.Wrap(exception.CodeUnknownError, e)
			c.JSON(ee.Code.HttpStatus(), ee)
		}
	}
}
