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

    "github.com/gin-gonic/gin"

    "github.com/arana-db/arana/pkg/admin"
    "github.com/arana-db/arana/pkg/boot"
)

func init() {
    admin.Register(func(router gin.IRoutes) {
        router.GET("/tenants", ListTenants)
        router.POST("/tenants", CreateTenant)
        router.GET("/tenants/:tenant", GetTenant)
        router.PUT("/tenants/:tenant", UpdateTenant)
        router.DELETE("/tenants/:tenant", RemoveTenant)
    })
}

func ListTenants(c *gin.Context) {
    service := admin.GetService(c)
    tenants, err := service.ListTenants(context.Background())
    if err != nil {
        _ = c.Error(err)
        return
    }
    c.JSON(http.StatusOK, tenants)
}

func CreateTenant(c *gin.Context) {
    service := admin.GetService(c)
    var tenantBody *boot.TenantBody
    if err := c.ShouldBindJSON(&tenantBody); err == nil {
        err := service.UpsertTenant(context.Background(), "", tenantBody)
        if err != nil {
            _ = c.Error(err)
            return
        }
        c.JSON(http.StatusCreated, nil)
    } else {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    }
}

func GetTenant(c *gin.Context) {
    service := admin.GetService(c)
    tenant := c.Param("tenant")
    data, err := service.GetTenant(context.Background(), tenant)
    if err != nil {
        _ = c.Error(err)
        return
    }
    c.JSON(http.StatusOK, data)
}

func UpdateTenant(c *gin.Context) {
    service := admin.GetService(c)
    tenant := c.Param("tenant")
    var tenantBody *boot.TenantBody
    if err := c.ShouldBindJSON(&tenantBody); err == nil {
        err := service.UpsertTenant(context.Background(), tenant, tenantBody)
        if err != nil {
            _ = c.Error(err)
            return
        }
        c.JSON(http.StatusOK, nil)
    } else {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
    }
}

func RemoveTenant(c *gin.Context) {
    service := admin.GetService(c)
    tenant := c.Param("tenant")
    err := service.RemoveTenant(context.Background(), tenant)
    if err != nil {
        _ = c.Error(err)
        return
    }
    c.JSON(http.StatusNoContent, nil)
}
