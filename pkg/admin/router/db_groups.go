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

	"github.com/pkg/errors"
)

import (
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/admin/exception"
)

func init() {
	admin.Register(func(router admin.Router) {
		router.GET("/tenants/:tenant/groups", ListGroups)
		router.GET("/tenants/:tenant/clusters/:cluster/groups", ListGroups)
		router.POST("/tenants/:tenant/clusters/:cluster/groups", CreateGroup)
		router.GET("/tenants/:tenant/clusters/:cluster/groups/:group", GetGroup)
		router.PUT("/tenants/:tenant/clusters/:cluster/groups/:group", UpdateGroup)
		router.DELETE("/tenants/:tenant/clusters/:cluster/groups/:group", RemoveGroup)
	})
}

func CreateGroup(c *gin.Context) error {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	cluster := c.Param("cluster")
	var group *admin.GroupDTO
	if err := c.ShouldBindJSON(&group); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertGroup(context.Background(), tenantName, cluster, group.Name, group)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}

func ListGroups(c *gin.Context) error {
	var (
		service             = admin.GetService(c)
		tenantName, cluster = c.Param("tenant"), c.Param("cluster")
		clusterNames        []string
		groups              []*admin.GroupDTO
		err                 error
	)

	if len(cluster) > 0 {
		clusterNames = append(clusterNames, cluster)
	} else {
		var clusters []*admin.ClusterDTO
		if clusters, err = service.ListClusters(context.Background(), tenantName); err != nil {
			return err
		}
		for i := range clusters {
			clusterNames = append(clusterNames, clusters[i].Name)
		}
	}

	for i := range clusterNames {
		var nextGroups []*admin.GroupDTO
		if nextGroups, err = service.ListDBGroups(context.Background(), tenantName, clusterNames[i]); err != nil {
			return err
		}
		groups = append(groups, nextGroups...)
	}

	c.JSON(http.StatusOK, groups)
	return nil
}

func GetGroup(c *gin.Context) error {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	cluster := c.Param("cluster")
	group := c.Param("group")

	groups, err := service.ListDBGroups(context.Background(), tenant, cluster)
	if err != nil {
		return err
	}

	var res *admin.GroupDTO
	for i := range groups {
		if groups[i].Name == group {
			res = groups[i]
			break
		}
	}

	if res == nil {
		return errors.Errorf("no such db group '%s'", group)
	}

	c.JSON(http.StatusOK, res)
	return nil
}

func UpdateGroup(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster, groupName := c.Param("tenant"), c.Param("cluster"), c.Param("group")

	var group admin.GroupDTO
	if err := c.ShouldBindJSON(&group); err != nil {
		return exception.Wrap(exception.CodeInvalidParams, err)
	}

	err := service.UpsertGroup(context.Background(), tenant, cluster, groupName, &group)
	if err != nil {
		return err
	}

	c.JSON(http.StatusOK, nil)
	return nil
}

func RemoveGroup(c *gin.Context) error {
	service := admin.GetService(c)
	tenant, cluster, group := c.Param("tenant"), c.Param("cluster"), c.Param("group")

	err := service.RemoveGroup(context.Background(), tenant, cluster, group)
	if err != nil {
		return err
	}
	c.JSON(http.StatusOK, nil)
	return nil
}
