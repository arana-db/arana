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
		router.GET("/tenants/:tenant/clusters", ListClusters)
		router.POST("/tenants/:tenant/clusters", CreateCluster)
		router.GET("/tenants/:tenant/clusters/:cluster", GetCluster)
		router.PUT("/tenants/:tenant/clusters/:cluster", UpdateCluster)
		router.POST("/tenants/:tenant/clusters/:cluster", ExtendCluster)
		router.DELETE("/tenants/:tenant/clusters/:cluster", RemoveCluster)
	})
}

func ListClusters(c *gin.Context) error {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	clusters, err := service.ListClusters(c, tenantName)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, clusters)
	return nil
}

func GetCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")

	clusters, err := service.ListClusters(c, tenant)
	if err != nil {
		return err
	}

	for i := range clusters {
		if clusters[i].Name == cluster {
			c.JSON(http.StatusOK, clusters[i])
			return nil
		}
	}

	return exception.New(exception.CodeNotFound, "no such cluster '%s'", cluster)
}

func CreateCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")

	var cluster admin.ClusterDTO
	if err := c.ShouldBindJSON(&cluster); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertCluster(c, tenant, cluster.Name, &cluster)
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
	var clusterBody admin.ClusterDTO
	if err := c.ShouldBindJSON(&clusterBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertCluster(c, tenant, cluster, &clusterBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, "success")
	return nil
}

func ExtendCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	var clusterBody admin.ClusterDTO
	if err := c.ShouldBindJSON(&clusterBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.ExtendCluster(c, tenant, cluster, &clusterBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, "success")
	return nil
}

func RemoveCluster(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	err := service.RemoveCluster(c, tenant, cluster)
	if err != nil {
		return err
	}
	c.Status(http.StatusNoContent)
	return nil
}
