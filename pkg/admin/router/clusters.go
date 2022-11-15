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
		router.GET("/tenants/:tenant/clusters", ListClusters)
		router.POST("/tenants/:tenant/clusters/:cluster", CreateCluster)
		router.GET("/tenants/:tenant/clusters/:cluster", GetCluster)
		router.PUT("/tenants/:tenant/clusters/:cluster", UpdateCluster)
		router.DELETE("/tenants/:tenant/clusters/:cluster", RemoveCluster)
	})
}

func ListClusters(c *gin.Context) error {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	clusters, err := service.ListClusters(context.Background(), tenantName)
	if err != nil {
		return err
	}
	var res []*config.DataSourceCluster
	for _, it := range clusters {
		cluster, err := service.GetDataSourceCluster(context.Background(), tenantName, it)
		if err != nil {
			return err
		}
		res = append(res, cluster)
	}

	c.JSON(http.StatusOK, res)
	return nil
}

func GetCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	data, err := service.GetCluster(context.Background(), tenant, cluster)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, data)
	return nil
}

func CreateCluster(c *gin.Context) error {
	var (
		service     = admin.GetService(c)
		tenant      = c.Param("tenant")
		cluster     = c.Param("cluster")
		clusterBody *boot.ClusterBody
	)
	if err := c.ShouldBindJSON(&clusterBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertCluster(context.Background(), tenant, cluster, clusterBody)
	if err != nil {
		return err
	}

	c.JSON(http.StatusOK, "ok")

	return nil
}

func UpdateCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	var clusterBody *boot.ClusterBody
	if err := c.ShouldBindJSON(&clusterBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertCluster(context.Background(), tenant, cluster, clusterBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func RemoveCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	err := service.RemoveCluster(context.Background(), tenant, cluster)
	if err != nil {
		return err
	}
	c.JSON(http.StatusNoContent, nil)
	return nil
}
