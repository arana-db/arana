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
	"github.com/arana-db/arana/pkg/boot"
)

func init() {
	admin.Register(func(router gin.IRoutes) {
		router.GET("/tenants/:tenant/nodes", ListNodes)
		router.POST("/tenants/:tenant/nodes", CreateNode)
		router.GET("/tenants/:tenant/nodes/:node", GetNode)
		router.PUT("/tenants/:tenant/nodes/:node", UpdateNode)
		router.DELETE("/tenants/:tenant/nodes/:node", RemoveNode)
	})
}

func ListNodes(c *gin.Context) {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	nodes, err := service.ListNodes(context.Background(), tenantName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, nodes)
}

func GetNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	data, err := service.GetNode(context.Background(), tenant, node)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, data)
}

func CreateNode(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	var node *boot.NodeBody
	if err := c.ShouldBindJSON(&node); err == nil {
		//TODO how to get cluster name?
		err := service.UpsertNode(context.Background(), tenant, "", node)
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
		err := service.UpsertNO(context.Background(), tenant, node, nodeBody)
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
	err := service.RemoveNode(context.Background(), tenant, node)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusNoContent, nil)
}
