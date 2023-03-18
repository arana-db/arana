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
)

func init() {
	admin.Register(func(router admin.Router, openRouter admin.Router) {
		openRouter.GET("/tenants", ListTenants)
		router.GET("/tenants", ListTenants)
		router.POST("/tenants", CreateTenant)
		router.GET("/tenants/:tenant", GetTenant)
		router.PUT("/tenants/:tenant", UpdateTenant)
		router.DELETE("/tenants/:tenant", RemoveTenant)
	})
}

func ListTenants(c *gin.Context) error {
	service := admin.GetService(c)

	tenants, err := service.ListTenants(c)
	if err != nil {
		return err
	}

	if tenants == nil {
		tenants = []*admin.TenantDTO{}
	}

	c.JSON(http.StatusOK, tenants)

	return nil
}

func CreateTenant(c *gin.Context) error {
	service := admin.GetService(c)
	var tenantBody admin.TenantDTO
	if err := c.ShouldBindJSON(&tenantBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	if err := validateTenantName(tenantBody.Name); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertTenant(c, tenantBody.Name, &tenantBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusCreated, "success")
	return nil
}

func GetTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")

	tenants, err := service.ListTenants(c)
	if err != nil {
		return err
	}

	var res *admin.TenantDTO
	for i := range tenants {
		if tenants[i].Name == tenant {
			res = tenants[i]
			break
		}
	}

	if res == nil {
		return exception.New(exception.CodeNotFound, "no such tenant `%s`", tenant)
	}

	c.JSON(http.StatusOK, res)
	return nil
}

func UpdateTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")

	var upsert admin.TenantDTO
	if err := c.ShouldBindJSON(&upsert); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertTenant(c, tenant, &upsert)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, "success")
	return nil
}

func RemoveTenant(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")

	if err := service.RemoveTenant(c, tenant); err != nil {
		return err
	}

	c.Status(http.StatusNoContent)
	return nil
}
