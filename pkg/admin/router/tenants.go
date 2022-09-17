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
	"context"
	"net/http"
)

import (
	"github.com/gin-gonic/gin"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/admin/exception"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/config"
)

func init() {
	admin.Register(func(router admin.Router) {
		router.GET("/tenants", ListTenants)
		router.POST("/tenants", CreateTenant)
		router.GET("/tenants/:tenant", GetTenant)
		router.PUT("/tenants/:tenant", UpdateTenant)
		router.DELETE("/tenants/:tenant", RemoveTenant)
	})
}

func ListTenants(c *gin.Context) error {
	service := admin.GetService(c)
	tenants, err := service.ListTenants(context.Background())
	if err != nil {
		return err
	}

	var res []gin.H
	for _, it := range tenants {
		tenant, err := service.GetTenant(context.Background(), it)
		if err != nil {
			return err
		}
		res = append(res, convertTenantToVO(tenant))
	}

	c.JSON(http.StatusOK, res)
	return nil
}

func CreateTenant(c *gin.Context) error {
	service := admin.GetService(c)
	var tenantBody *boot.TenantBody
	if err := c.ShouldBindJSON(&tenantBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}
	err := service.UpsertTenant(context.Background(), "", tenantBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusCreated, nil)
	return nil
}

func GetTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	data, err := service.GetTenant(context.Background(), tenant)
	if err != nil {
		return err
	}

	if data == nil {
		return exception.New(exception.CodeNotFound, "no such tenant `%s`", tenant)
	}

	c.JSON(http.StatusOK, convertTenantToVO(data))
	return nil
}

func UpdateTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	var tenantBody *boot.TenantBody
	if err := c.ShouldBindJSON(&tenantBody); err == nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertTenant(context.Background(), tenant, tenantBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func RemoveTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	err := service.RemoveTenant(context.Background(), tenant)
	if err != nil {
		return err
	}
	c.JSON(http.StatusNoContent, nil)
	return nil
}

func convertTenantToVO(tenant *config.Tenant) gin.H {
	users := make([]gin.H, 0, len(tenant.Users))
	for _, next := range tenant.Users {
		users = append(users, gin.H{
			"username": next.Username,
			"password": next.Password,
		})
	}
	return gin.H{
		"name":  tenant.Name,
		"users": users,
	}
}

