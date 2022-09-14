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
	"github.com/arana-db/arana/pkg/boot"
	"github.com/arana-db/arana/pkg/config"
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
	var results []config.Node
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	clusters, err := service.ListClusters(c, tenantName)
	if err != nil {
		return err
	}
	for _, cluster := range clusters {
		groups, err := service.ListGroups(c, tenantName, cluster)
		if err != nil {
			return err
		}
		for _, group := range groups {
			nodesArray, err := service.ListNodes(c, tenantName, cluster, group)
			if err != nil {
				return err
			}
			for _, node := range nodesArray {
				result, err := service.GetNode(c, tenantName, cluster, group, node)
				if err != nil {
					return err
				}
				results = append(results, *result)
			}
		}
	}
	c.JSON(http.StatusOK, results)
	return nil
}

func GetNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	clusters, err := service.ListClusters(c, tenant)
	if err != nil {
		return err
	}
	var data *config.Node
	for _, cluster := range clusters {
		groups, err := service.ListGroups(c, tenant, cluster)
		if err != nil {
			return err
		}
		for _, group := range groups {
			data, err = service.GetNode(c, tenant, cluster, group, node)
			if err != nil {
				return err
			}
		}
	}
	c.JSON(http.StatusOK, data)
	return nil
}

func CreateNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	var node *boot.NodeBody
	if err := c.ShouldBindJSON(&node); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertNode(c, tenant, "", node)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func UpdateNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	var nodeBody *boot.NodeBody
	if err := c.ShouldBindJSON(&nodeBody); err == nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertNode(c, tenant, node, nodeBody)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func RemoveNode(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	node := c.Param("node")
	err := service.RemoveNode(c, tenant, node)
	if err != nil {
		return err
	}
	c.JSON(http.StatusNoContent, nil)
	return nil
}
