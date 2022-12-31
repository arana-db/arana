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

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/admin/exception"
)

func init() {
	admin.Register(func(router admin.Router) {
		router.GET("/tenants/:tenant/nodes", ListNodes)
		router.POST("/tenants/:tenant/nodes", CreateNode)
		router.GET("/tenants/:tenant/nodes/:node", GetNode)
		router.PUT("/tenants/:tenant/nodes/:node", UpdateNode)
		router.DELETE("/tenants/:tenant/nodes/:node", RemoveNode)
	})
}

func ListNodes(c *gin.Context) error {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")

	nodes, err := service.ListNodes(c, tenantName)
	if err != nil {
		return err
	}

	if nodes == nil {
		nodes = []*admin.NodeDTO{}
	}

	c.JSON(http.StatusOK, nodes)
	return nil
}

func GetNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	nodeName := c.Param("node")

	nodes, err := service.ListNodes(c, tenant)
	if err != nil {
		return err
	}

	var node *admin.NodeDTO
	for i := range nodes {
		if nodes[i].Name == nodeName {
			node = nodes[i]
			break
		}
	}

	if node == nil {
		return errors.Errorf("no such node '%s'", nodeName)
	}

	c.JSON(http.StatusOK, node)

	return nil
}

func CreateNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")

	var nodeBody admin.NodeDTO
	if err := c.ShouldBindJSON(&nodeBody); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertNode(c, tenant, nodeBody.Name, &nodeBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, "success")
	return nil
}

func UpdateNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")

	var dto admin.NodeDTO
	if err := c.ShouldBindJSON(&dto); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertNode(c, tenant, node, &dto)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, "success")
	return nil
}

func RemoveNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, node := c.Param("tenant"), c.Param("node")

	if err := service.RemoveNode(c, tenant, node); err != nil {
		return err
	}

	c.Status(http.StatusNoContent)
	return nil
}
