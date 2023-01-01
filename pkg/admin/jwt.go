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
	"time"
)

import (
	jwt "github.com/appleboy/gin-jwt/v2"

	"github.com/gin-gonic/gin"
)

import (
	"github.com/arana-db/arana/pkg/security"
)

const (
	_identityKey = "key"
	_tenant      = "tenant"
	_username    = "username"
	_password    = "password"
)

type LoginPayload struct {
	Tenant   string `form:"tenant" json:"tenant"`
	Username string `form:"username" json:"username" binding:"required"`
	Password string `form:"password" json:"password" binding:"required"`
}

func NewAuthMiddleware(server *Server, realm, secretKey string) (*jwt.GinJWTMiddleware, error) {
	conf := server.service.(*myConfigService)
	for _, tenant := range conf.tenantOp.ListTenants() {
		center, err := conf.getCenter(context.Background(), tenant)
		if err != nil {
			return nil, err
		}
		all, err := center.LoadAll(context.Background())
		if err != nil {
			return nil, err
		}
		for _, user := range all.Users {
			security.DefaultTenantManager().PutUser(tenant, user)
		}
	}

	return jwt.New(&jwt.GinJWTMiddleware{
		Realm:       realm,
		Key:         []byte(secretKey),
		Timeout:     time.Hour,
		MaxRefresh:  time.Hour,
		IdentityKey: _identityKey,
		PayloadFunc: func(data interface{}) jwt.MapClaims {
			if v, ok := data.(*LoginPayload); ok {
				return jwt.MapClaims{
					_identityKey: v,
				}
			}
			return jwt.MapClaims{}
		},
		IdentityHandler: func(c *gin.Context) interface{} {
			claims := jwt.ExtractClaims(c)
			identityMap := claims[_identityKey].(map[string]interface{})
			return &LoginPayload{
				Tenant:   identityMap[_tenant].(string),
				Username: identityMap[_username].(string),
				Password: identityMap[_password].(string),
			}
		},
		Authenticator: func(c *gin.Context) (interface{}, error) {
			var loginPayload LoginPayload
			if err := c.ShouldBind(&loginPayload); err != nil {
				return "", jwt.ErrMissingLoginValues
			}
			tenant, username, password := loginPayload.Tenant, loginPayload.Username, loginPayload.Password

			if validateUser(tenant, username, password) {
				return &loginPayload, nil
			}

			return nil, jwt.ErrFailedAuthentication
		},
		Authorizator: func(data interface{}, c *gin.Context) bool {
			v, ok := data.(*LoginPayload)
			return ok && validateUser(v.Tenant, v.Username, v.Password)
		},
		Unauthorized: func(c *gin.Context, code int, message string) {
			c.JSON(code, gin.H{
				"code":    code,
				"message": message,
			})
		},
		// TokenLookup is a string in the form of "<source>:<name>" that is used
		// to extract token from the request.
		// Optional. Default value "header:Authorization".
		// Possible values:
		// - "header:<name>"
		// - "query:<name>"
		// - "cookie:<name>"
		// - "param:<name>"
		TokenLookup: "header: Authorization, query: token, cookie: jwt",
		// TokenLookup: "query:token",
		// TokenLookup: "cookie:token",

		// TokenHeadName is a string in the header. Default value is "Bearer"
		TokenHeadName: "Bearer",

		// TimeFunc provides the current time. You can override it to use another time value. This is useful for testing or if your server uses a different time zone than your tokens.
		TimeFunc: time.Now,
	})
}

func validateUser(tenant, username, password string) bool {
	tm := security.DefaultTenantManager()
	// validate normal user
	if tenant != "" {
		user, exist := tm.GetUser(tenant, username)
		if exist && user.Password == password {
			return true
		}
		return false
	}

	// validate supervisor
	supervisor := tm.GetSupervisor()
	if supervisor == nil {
		return false
	}

	if username != supervisor.Username || password != supervisor.Password {
		return false
	}

	return true
}
