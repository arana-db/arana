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

package router

import (
	"net/http"
)

import (
	"github.com/gin-gonic/gin"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/admin/exception"
	"github.com/arana-db/arana/pkg/config"
)

func init() {
	admin.Register(func(router admin.Router) {
		router.GET("/tenants/:tenant/users", ListUser)
		router.POST("/tenants/:tenant/users", CreateUser)
		router.PUT("/tenants/:tenant/users/:user", UpdateUser)
		router.DELETE("/tenants/:tenant/users/:user", DeleteUser)
	})
}

func ListUser(ctx *gin.Context) error {
	tenant := ctx.Param("tenant")

	tenants, err := admin.GetService(ctx).ListTenants(ctx)
	if err != nil {
		return err
	}

	for i := range tenants {
		if tenants[i].Name == tenant {
			ctx.JSON(http.StatusOK, tenants[i].Users)
			return nil
		}
	}

	ctx.JSON(http.StatusOK, []*config.User{})
	return nil
}

func DeleteUser(c *gin.Context) error {
	tenant := c.Param("tenant")
	username := c.Param("user")

	if err := admin.GetService(c).RemoveUser(c, tenant, username); err != nil {
		return err
	}

	c.JSON(http.StatusNoContent, nil)
	return nil
}

func UpdateUser(c *gin.Context) error {
	tenant := c.Param("tenant")
	username := c.Param("user")

	var user config.User
	if err := c.ShouldBindJSON(&user); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	if !validatePassword(user.Password) {
		return exception.New(exception.CodeInvalidParams, "bad password format")
	}

	if err := admin.GetService(c).UpsertUser(c, tenant, &user, username); err != nil {
		return err
	}

	c.JSON(http.StatusOK, "success")

	return nil
}

func CreateUser(c *gin.Context) error {
	tenant := c.Param("tenant")

	var user config.User
	if err := c.ShouldBindJSON(&user); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	if !validateNormalName(user.Username) {
		return exception.New(exception.CodeInvalidParams, "bad username format: %s", user.Username)
	}

	if !validatePassword(user.Password) {
		return exception.New(exception.CodeInvalidParams, "bad password format")
	}

	tenants, err := admin.GetService(c).ListTenants(c)
	if err != nil {
		return err
	}

	// check username conflict
	for _, t := range tenants {
		if t.Name == tenant {
			for _, u := range t.Users {
				if u.Username == user.Username {
					return exception.New(exception.CodeServerError, "user '%s' exists already", user.Username)
				}
			}
		}
	}

	if err := admin.GetService(c).UpsertUser(c, tenant, &user, user.Username); err != nil {
		return err
	}

	c.JSON(http.StatusOK, "success")

	return nil
}
