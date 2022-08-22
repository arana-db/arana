package router

import (
	"context"
	"github.com/arana-db/arana/pkg/admin"
	"github.com/arana-db/arana/pkg/boot"
	"github.com/gin-gonic/gin"
	"net/http"
)

func init() {
	admin.Register(func(router gin.IRoutes) {
		router.POST("/tenants/:tenant/groups", CreateGroup)
		router.GET("/tenants/:tenant/groups", ListGroups)
		router.GET("/tenants/:tenant/groups/:group", GetGroup)
		router.PUT("/tenants/:tenant/groups/:group", UpdateGroup)
		router.DELETE("/tenants/:tenant/groups/:group", RemoveGroup)
	})
}

func CreateGroup(c *gin.Context) {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	var group *boot.GroupBody
	if err := c.ShouldBindJSON(&group); err == nil {
		err := service.UpsertGroup(context.Background(), tenantName, "", "", group)
		if err != nil {
			_ = c.Error(err)
			return
		}
		c.JSON(http.StatusOK, nil)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
}

func ListGroups(c *gin.Context) {
	service := admin.GetService(c)
	tenantName := c.Param("tenant")
	groups, err := service.ListGroups(context.Background(), tenantName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, groups)
}

func GetGroup(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	group := c.Param("group")
	data, err := service.GetGroup(context.Background(), tenant, "", group)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, data)
}

func UpdateGroup(c *gin.Context) {
	service := admin.GetService(c)
	tenant := c.Param("tenant")
	group := c.Param("group")
	var groupBody *boot.GroupBody
	if err := c.ShouldBindJSON(&groupBody); err == nil {
		err := service.UpsertGroup(context.Background(), tenant, "", group, groupBody)
		if err != nil {
			_ = c.Error(err)
			return
		}
		c.JSON(http.StatusOK, nil)
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
}

func RemoveGroup(c *gin.Context) {
	service := admin.GetService(c)
	tenant, group := c.Param("tenant"), c.Param("group")

	err := service.RemoveGroup(context.Background(), tenant, "", group)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, nil)
}
