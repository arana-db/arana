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
)

func init() {
	admin.Register(func(router admin.Router) {
		router.GET("/tenants/:tenant/clusters/:cluster/tables", ListTables)
		router.POST("/tenants/:tenant/clusters/:cluster/tables", CreateTable)
		router.GET("/tenants/:tenant/clusters/:cluster/tables/:table", GetTable)
		router.PUT("/tenants/:tenant/clusters/:cluster/tables/:table", UpsertTable)
		router.DELETE("/tenants/:tenant/clusters/:cluster/tables/:table", RemoveTable)
	})
}

func ListTables(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster := c.Param("tenant"), c.Param("cluster")

	tables, err := service.ListTables(context.Background(), tenant, cluster)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, tables)
	return nil
}

func CreateTable(c *gin.Context) error {
	service := admin.GetService(c)
	var (
		tenant    = c.Param("tenant")
		cluster   = c.Param("cluster")
		tableBody admin.TableDTO
	)

	if err := c.ShouldBindJSON(&tableBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertTable(context.Background(), tenant, cluster, tableBody.Name, &tableBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func GetTable(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster, table := c.Param("tenant"), c.Param("cluster"), c.Param("table")

	tables, err := service.ListTables(context.Background(), tenant, cluster)
	if err != nil {
		return err
	}
	var data *admin.TableDTO
	for i := range tables {
		if tables[i].Name == table {
			data = tables[i]
			break
		}
	}

	if data == nil {
		return exception.New(exception.CodeNotFound, "no such table `%s`", table)
	}

	c.JSON(http.StatusOK, data)
	return nil
}

func UpsertTable(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster, table := c.Param("tenant"), c.Param("cluster"), c.Param("table")
	var tableBody admin.TableDTO
	if err := c.ShouldBindJSON(&tableBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertTable(context.Background(), tenant, cluster, table, &tableBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func RemoveTable(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster, table := c.Param("tenant"), c.Param("cluster"), c.Param("table")

	err := service.RemoveTable(context.Background(), tenant, cluster, table)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}
