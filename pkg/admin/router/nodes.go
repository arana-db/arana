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
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/config"
)

func init() {
	admin.Register(func(router gin.IRoutes) {
		if e, ok := router.(*gin.RouterGroup); ok {

			routerGroup := e.Group("/")
			routerGroup.GET("/tenants/:tenant/nodes", ListNodes)
			routerGroup.POST("/tenants/:tenant/nodes", CreateNode)
			routerGroup.GET("/tenants/:tenant/nodes/:node", GetNode)
			routerGroup.PUT("/tenants/:tenant/nodes/:node", UpdateNode)
			routerGroup.DELETE("/tenants/:tenant/nodes/:node", RemoveNode)

		}

	})
}

func ListNodes(c *gin.Context) {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	clusters, err := service.ListClusters(c, tenantName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var data []string
	for _, cluster := range clusters {
		groups, err := service.ListGroups(c, tenantName, cluster)
		if err != nil {
			_ = c.Error(err)
			continue
		}
		for _, group := range groups {
			temp, err := service.ListNodes(c, tenantName, cluster, group)
			if err != nil {
				_ = c.Error(err)
				continue
			} else {
				data = append(data, temp...)
			}
		}
	}
	c.JSON(http.StatusOK, data)
}

func GetNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	clusters, err := service.ListClusters(c, tenant)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var data *config.Node
	for _, cluster := range clusters {
		groups, err := service.ListGroups(c, tenant, cluster)
		if err != nil {
			_ = c.Error(err)
			continue
		}
		for _, group := range groups {
			data, err = service.GetNode(c, tenant, cluster, group, node)
			if err != nil {
				_ = c.Error(err)
				continue
			}
		}
	}
	c.JSON(http.StatusOK, data)
}

func CreateNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	var node *boot.NodeBody
	if err := c.ShouldBindJSON(&node); err == nil {
		err := service.UpsertNode(c, tenant, "", node)
		if err != nil {
			_ = c.Error(err)
			return
		}
		c.JSON(http.StatusOK, nil)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
}

func UpdateNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	var nodeBody *boot.NodeBody
	if err := c.ShouldBindJSON(&nodeBody); err == nil {
		err := service.UpsertNode(c, tenant, node, nodeBody)
		if err != nil {
			_ = c.Error(err)
			return
		}
		c.JSON(http.StatusOK, nil)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
}

func RemoveNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	err := service.RemoveNode(c, tenant, node)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusNoContent, nil)
}
