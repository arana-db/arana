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

package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

import (
	"github.com/creasty/defaults"

	perrors "github.com/pkg/errors"

	"golang.org/x/exp/slices"
)

import (
	"github.com/arana-db/arana/pkg/config"
	"github.com/arana-db/arana/pkg/util/log"
	"github.com/arana-db/arana/pkg/util/misc"
)

var _ ConfigService = (*MyConfigService)(nil)

var (
	errNoSuchTenant  = errors.New("no such tenant")
	errNoSuchCluster = errors.New("no such cluster")
	errNoSuchGroup   = errors.New("no such group")
)

type MyConfigService struct {
	TenantOp config.TenantOperator
	centers  sync.Map // map[string]*lazyCenter
}

func (cs *MyConfigService) RemoveUser(ctx context.Context, tenant string, username string) error {
	return cs.TenantOp.RemoveTenantUser(tenant, username)
}

func (cs *MyConfigService) UpsertUser(ctx context.Context, tenant string, user *config.User, username string) error {
	if username != "" && username != user.Username {
		return cs.TenantOp.UpdateTenantUser(tenant, user.Username, user.Password, username)
	}
	return cs.TenantOp.CreateTenantUser(tenant, user.Username, user.Password)
}

func (cs *MyConfigService) ListTenants(ctx context.Context) ([]*TenantDTO, error) {
	tenants := cs.TenantOp.ListTenants()

	ret := make([]*TenantDTO, 0, len(tenants))
	for _, tenant := range tenants {
		center, err := cs.getCenter(ctx, tenant)
		if err != nil {
			return nil, perrors.WithStack(err)
		}

		next, err := center.Load(ctx, config.ConfigItemUsers)
		if err != nil {
			return nil, perrors.WithStack(err)
		}

		var users []*config.User
		if next != nil {
			users = make([]*config.User, len(next.Users))
			copy(users, next.Users)
			slices.SortFunc(users, func(a, b *config.User) bool {
				return strings.Compare(a.Username, b.Username) < 0
			})
		} else {
			users = []*config.User{}
		}

		ret = append(ret, &TenantDTO{
			Name:  tenant,
			Users: users,
		})
	}

	slices.SortFunc(ret, func(a, b *TenantDTO) bool {
		return strings.Compare(a.Name, b.Name) < 0
	})

	return ret, nil
}

func (cs *MyConfigService) ListNodes(ctx context.Context, tenant string) ([]*NodeDTO, error) {
	ct, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	cfg, err := ct.Load(ctx, config.ConfigItemNodes)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	if cfg == nil || len(cfg.Nodes) < 1 {
		return nil, nil
	}

	ret := make([]*NodeDTO, 0, len(cfg.Nodes))
	for _, n := range cfg.Nodes {
		ret = append(ret, &NodeDTO{
			Name:       n.Name,
			Host:       n.Host,
			Port:       n.Port,
			Username:   n.Username,
			Password:   n.Password,
			Database:   n.Database,
			Weight:     n.Weight,
			Parameters: n.Parameters,
			ConnProps:  n.ConnProps,
			Labels:     n.Labels,
		})
	}

	slices.SortFunc(ret, func(a, b *NodeDTO) bool {
		return strings.Compare(a.Name, b.Name) < 0
	})

	return ret, nil
}

func (cs *MyConfigService) ListClusters(ctx context.Context, tenant string) ([]*ClusterDTO, error) {
	ct, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return nil, perrors.WithStack(err)
	}
	cfg, err := ct.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	if cfg == nil || len(cfg.DataSourceClusters) < 1 {
		return nil, nil
	}

	ret := make([]*ClusterDTO, 0, len(cfg.DataSourceClusters))
	for _, next := range cfg.DataSourceClusters {
		dto := &ClusterDTO{
			Name:        next.Name,
			Type:        next.Type,
			SqlMaxLimit: next.SqlMaxLimit,
			Parameters:  next.Parameters,
		}

		for i := range next.Groups {
			dto.Groups = append(dto.Groups, next.Groups[i].Name)
		}

		sort.Strings(dto.Groups)

		ret = append(ret, dto)
	}

	slices.SortFunc(ret, func(a, b *ClusterDTO) bool {
		return strings.Compare(a.Name, b.Name) < 0
	})

	return ret, nil
}

func (cs *MyConfigService) ListDBGroups(ctx context.Context, tenant, cluster string) ([]*GroupDTO, error) {
	ct, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	cfg, err := ct.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	if cfg == nil || len(cfg.DataSourceClusters) < 1 {
		return nil, nil
	}

	var d *config.DataSourceCluster
	for i := range cfg.DataSourceClusters {
		if cfg.DataSourceClusters[i].Name == cluster {
			d = cfg.DataSourceClusters[i]
			break
		}
	}

	if d == nil {
		return nil, errNoSuchCluster
	}

	ret := make([]*GroupDTO, 0, len(d.Groups))
	for i := range d.Groups {
		ret = append(ret, &GroupDTO{
			ClusterName: cluster,
			Name:        d.Groups[i].Name,
			Nodes:       d.Groups[i].Nodes,
		})
	}

	slices.SortFunc(ret, func(a, b *GroupDTO) bool {
		return strings.Compare(a.Name, b.Name) < 0
	})

	return ret, nil
}

func (cs *MyConfigService) ListTables(ctx context.Context, tenant, cluster string) ([]*TableDTO, error) {
	ct, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return nil, errNoSuchTenant
	}

	cfg, err := ct.Load(ctx, config.ConfigItemShardingRule)
	if err != nil {
		return nil, perrors.WithStack(err)
	}

	if cfg == nil || cfg.ShardingRule == nil {
		return nil, nil
	}

	var ret []*TableDTO
	for _, next := range cfg.ShardingRule.Tables {
		db, tbl, _ := misc.ParseTable(next.Name)
		if len(db) < 1 || db != cluster {
			continue
		}

		ret = append(ret, &TableDTO{
			Name:           tbl,
			Sequence:       next.Sequence,
			DbRules:        next.DbRules,
			TblRules:       next.TblRules,
			Topology:       next.Topology,
			ShadowTopology: next.ShadowTopology,
			Attributes:     next.Attributes,
		})
	}

	slices.SortFunc(ret, func(a, b *TableDTO) bool {
		return strings.Compare(a.Name, b.Name) < 0
	})

	return ret, nil
}

func (cs *MyConfigService) UpsertTenant(ctx context.Context, tenant string, body *TenantDTO) error {
	if tenant != body.Name {
		cs.TenantOp.UpdateTenant(tenant, body.Name)
		return nil
	}
	if err := cs.TenantOp.CreateTenant(tenant); err != nil {
		return perrors.Wrapf(err, "failed to create tenant '%s'", tenant)
	}

	for _, next := range body.Users {
		if err := cs.TenantOp.CreateTenantUser(tenant, next.Username, next.Password); err != nil {
			return perrors.WithStack(err)
		}
	}

	return nil
}

func (cs *MyConfigService) RemoveTenant(ctx context.Context, tenant string) error {
	if err := cs.TenantOp.RemoveTenant(tenant); err != nil {
		return perrors.Wrapf(err, "failed to remove tenant '%s'", tenant)
	}
	return nil
}

func (cs *MyConfigService) UpsertCluster(ctx context.Context, tenant, cluster string, body *ClusterDTO) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	cfg, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return err
	}

	var (
		newClusters = make([]*config.DataSourceCluster, 0, len(cfg.DataSourceClusters))
		exist       = false
	)

	newClusters = append(newClusters, cfg.DataSourceClusters...)

	for _, newCluster := range newClusters {
		if newCluster.Name == cluster {
			exist = true
			newCluster.Name = body.Name
			newCluster.Type = body.Type
			newCluster.Parameters = body.Parameters
			newCluster.SqlMaxLimit = body.SqlMaxLimit
			break
		}
	}
	if !exist {
		newClusters = append(newClusters, &config.DataSourceCluster{
			Name:        cluster,
			Type:        body.Type,
			SqlMaxLimit: body.SqlMaxLimit,
			Parameters:  body.Parameters,
			Groups:      nil,
		})
	}
	cfg.DataSourceClusters = newClusters

	err = op.Write(ctx, config.ConfigItemClusters, cfg)
	if err != nil {
		return err
	}

	return nil
}

func (cs *MyConfigService) buildDTSJson(ctx context.Context, tenant, cluster, src, dst string, srcNode, dstNode *NodeDTO, vtables []*TableDTO, idx int) map[string]interface{} {
	jobJson := make(map[string]interface{})
	jobBody := make(map[string]interface{})
	jobJson["Job"] = jobBody

	jobId := tenant + "-" + cluster + "-" + src + "-" + dst + "-" + time.Now().Format("20060102150405")
	jobBody["ID"] = jobId
	jobBody["Datacenters"] = []string{"dc1"}
	jobGroups := make([]interface{}, 0, 2)

	jobSrc := make(map[string]interface{})
	jobSrc["Name"] = "src"

	jobTasks := make([]interface{}, 0, 1)
	jobTask := make(map[string]interface{})
	jobTask["Name"] = "src"
	jobTask["Driver"] = "dtle"
	jobConfig := make(map[string]interface{})
	jobConfig["Gtid"] = ""
	jobReplicate := make([]interface{}, 0, 1)
	jobDatabase := make(map[string]interface{})
	jobDatabase["TableSchema"] = src
	jobDatabase["TableSchemaRename"] = dst
	jobTables := []map[string]string{}
	for i := range vtables {
		vTable := vtables[i]
		_, _, dbEnd, _ := config.ParseTopology(vTable.Topology.DbPattern)
		tbFormat, _, tbEnd, _ := config.ParseTopology(vTable.Topology.TblPattern)
		tableNum := int((tbEnd + 1) / (dbEnd + 1))
		for j := 0; j < tableNum; j++ {
			jobTable := map[string]string{}
			jobTable["TableName"] = fmt.Sprintf(tbFormat, idx*tableNum+j)
			jobTable["TableRename"] = fmt.Sprintf(tbFormat, idx*tableNum+j+tbEnd+1)
			jobTables = append(jobTables, jobTable)
		}
	}
	jobDatabase["Tables"] = jobTables

	jobReplicate = append(jobReplicate, jobDatabase)
	jobConfig["ReplicateDoDb"] = jobReplicate
	jobSrcConfig := make(map[string]interface{})
	jobSrcConfig["Host"] = srcNode.Host
	jobSrcConfig["Port"] = srcNode.Port
	jobSrcConfig["User"] = srcNode.Username
	jobSrcConfig["Password"] = srcNode.Password
	jobConfig["SrcConnectionConfig"] = jobSrcConfig
	jobDstConfig := make(map[string]interface{})
	jobDstConfig["Host"] = dstNode.Host
	jobDstConfig["Port"] = dstNode.Port
	jobDstConfig["User"] = dstNode.Username
	jobDstConfig["Password"] = dstNode.Password
	jobConfig["DestConnectionConfig"] = jobDstConfig
	jobTask["Config"] = jobConfig
	jobSrc["Tasks"] = append(jobTasks, jobTask)
	jobGroups = append(jobGroups, jobSrc)

	jobDst := make(map[string]interface{})
	jobDst["Name"] = "dest"
	jobTasks = make([]interface{}, 0, 1)
	jobTask = make(map[string]interface{})
	jobTask["Name"] = "dest"
	jobTask["Driver"] = "dtle"
	jobTask["Config"] = map[string]string{"DestType": "mysql"}
	jobDst["Tasks"] = append(jobTasks, jobTask)
	jobGroups = append(jobGroups, jobDst)
	jobBody["TaskGroups"] = jobGroups

	return jobJson
}

func (cs *MyConfigService) ExtendCluster(ctx context.Context, tenant, cluster string, body *ClusterDTO) error {
	//1、校验node和group，保证node和group翻倍（缩容将node和group减半，流程同理）
	groups, err := cs.ListDBGroups(ctx, tenant, cluster)
	if err != nil {
		return err
	}
	if len(groups) != len(body.Groups) {
		return perrors.Errorf("new groups is not equle to old groups")
	}
	vtables, err := cs.ListTables(ctx, tenant, cluster)
	if err != nil {
		return err
	}
	allNodes, err := cs.ListNodes(ctx, tenant)
	if err != nil {
		return err
	}

	//2、创建复制group（物理数据库）的dts任务
	//groups[0] --> body.Groups[0]
	//groups[1] --> body.Groups[1]
	//...
	httpClient := &http.Client{}
	dtsJobList := make([]map[string]interface{}, 0, len(groups))
	dtsEndpoint := config.BootOpts.Dts.Options["endpoints"].(string)
	for i := range groups {
		srcGroup := groups[i].Name
		var srcNode, dstNode *NodeDTO
		for n := range allNodes {
			if allNodes[n].Database == srcGroup {
				srcNode = allNodes[n]
			}
		}
		dstGroup := body.Groups[i]
		for n := range allNodes {
			if allNodes[n].Database == dstGroup {
				dstNode = allNodes[n]
			}
		}
		dtsJob := cs.buildDTSJson(ctx, tenant, cluster, srcGroup, dstGroup, srcNode, dstNode, vtables, i)
		if dtsJob == nil {
			return perrors.Errorf("failed to build DTS json parameter")
		}
		dtsJobList = append(dtsJobList, dtsJob)
		dtsJobJson, _ := json.Marshal(dtsJob)
		httpReq, err := http.NewRequest("POST", dtsEndpoint+"/v1/jobs", bytes.NewBuffer(dtsJobJson))
		if err != nil {
			return perrors.Errorf("failed to create POST http requst")
		}

		httpResp, err := httpClient.Do(httpReq)
		if err != nil {
			return perrors.Errorf("failed to start to replica source group")
		}
		httpResp.Body.Close()
	}

	//3、检查是否复制完毕
	for {
		time.Sleep(5 * time.Second)
		finished := false

		for i := range dtsJobList {
			dtsJob := dtsJobList[i]["Job"].(map[string]interface{})
			httpURL := dtsEndpoint + "/v1/job/" + dtsJob["ID"].(string)
			httpReq, err := http.NewRequest("GET", httpURL, nil)
			if err != nil {
				return perrors.Errorf("failed to create GET http requst")
			}

			httpResp, err := httpClient.Do(httpReq)
			if err != nil {
				return perrors.Errorf("failed to check replica source group")
			}

			//TODO: check Status
			finished = true
			httpResp.Body.Close()
		}

		if finished {
			break
		}
	}

	//4、断开并拒绝所有客户端连接

	//5、再次检查是否复制完毕

	//6、停止dts任务
	for i := range groups {
		dtsJob := dtsJobList[i]["Job"].(map[string]interface{})
		httpURL := dtsEndpoint + "/v1/job/" + dtsJob["ID"].(string)
		httpReq, err := http.NewRequest("DELETE", httpURL, nil)
		if err != nil {
			return perrors.Errorf("failed to create DELETE http requst")
		}

		httpResp, err := httpClient.Do(httpReq)
		if err != nil {
			return perrors.Errorf("failed to stop to replica source group")
		}
		httpResp.Body.Close()
	}

	//7、更新groups节点
	var groupBody GroupDTO
	var groupNode string
	for i := range body.Groups {
		groupNode = ""
		for n := range allNodes {
			if allNodes[n].Database == body.Groups[i] {
				groupNode = allNodes[n].Name
			}
		}
		if strings.Compare(groupNode, "") == 0 {
			continue
		}
		groupBody.ClusterName = cluster
		groupBody.Name = body.Groups[i]
		groupBody.Nodes = []string{groupNode}
		err = cs.UpsertGroup(ctx, tenant, cluster, groupBody.Name, &groupBody)
		if err != nil {
			return err
		}
	}

	//8、更新sharding路由
	var tableBody TableDTO
	for i := range vtables {
		vTable := vtables[i]
		_, _, dbEnd, err := config.ParseTopology(vTable.Topology.DbPattern)
		if err != nil {
			return err
		}
		_, _, tbEnd, err := config.ParseTopology(vTable.Topology.TblPattern)
		if err != nil {
			return err
		}
		dbTotal := 2 * (dbEnd + 1)
		tableTotal := 2 * (tbEnd + 1)

		tableBody.Name = vTable.Name
		tableBody.Sequence = vTable.Sequence
		tableBody.DbRules = []*config.Rule{
			{
				Columns: vTable.DbRules[0].Columns,
				Type:    vTable.DbRules[0].Type,
				Expr:    "$0 % " + fmt.Sprintf("%d", tableTotal) + " / " + fmt.Sprintf("%d", dbTotal),
			},
		}
		tableBody.TblRules = []*config.Rule{
			{
				Columns: vTable.TblRules[0].Columns,
				Type:    vTable.TblRules[0].Type,
				Expr:    "$0 % " + fmt.Sprintf("%d", tableTotal),
			},
		}
		tableBody.Topology = &config.Topology{
			DbPattern:  cluster + fmt.Sprintf("_${0000..%04d}", dbTotal-1),
			TblPattern: vTable.Name + fmt.Sprintf("_${0000..%04d}", tableTotal-1),
		}
		tableBody.ShadowTopology = vTable.ShadowTopology
		tableBody.Attributes = vTable.Attributes
		err = cs.UpsertTable(ctx, tenant, cluster, tableBody.Name, &tableBody)
		if err != nil {
			return err
		}
	}

	//9、接受客户端连接

	return nil
}

func (cs *MyConfigService) RemoveCluster(ctx context.Context, tenant, cluster string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	c, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return perrors.WithStack(err)
	}

	remainedDsClusters := make([]*config.DataSourceCluster, 0, len(c.DataSourceClusters)-1)
	for _, dsc := range c.DataSourceClusters {
		if dsc.Name != cluster {
			remainedDsClusters = append(remainedDsClusters, dsc)
		}
	}

	c.DataSourceClusters = remainedDsClusters
	err = op.Write(ctx, config.ConfigItemClusters, c)
	if err != nil {
		return err
	}

	return nil
}

func (cs *MyConfigService) UpsertNode(ctx context.Context, tenant, node string, body *NodeDTO) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	c, err := op.Load(ctx, config.ConfigItemNodes)
	if err != nil {
		return perrors.WithStack(err)
	}

	if old, ok := c.Nodes[node]; ok {
		delete(c.Nodes, node)
		c.Nodes[body.Name] = old
		old.Name = body.Name
		old.Host = body.Host
		old.Port = body.Port
		old.Username = body.Username
		old.Password = body.Password
		old.Database = body.Database
		old.Weight = body.Weight
		old.Parameters = body.Parameters
		old.ConnProps = body.ConnProps
		old.Labels = body.Labels
	} else {
		c.Nodes[body.Name] = &config.Node{
			Name:       body.Name,
			Host:       body.Host,
			Port:       body.Port,
			Username:   body.Username,
			Password:   body.Password,
			Database:   body.Database,
			Parameters: body.Parameters,
			ConnProps:  body.ConnProps,
			Weight:     body.Weight,
			Labels:     body.Labels,
		}
	}

	if err := op.Write(ctx, config.ConfigItemNodes, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) RemoveNode(ctx context.Context, tenant, node string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	c, err := op.Load(ctx, config.ConfigItemNodes)
	if err != nil {
		return perrors.WithStack(err)
	}

	if _, ok := c.Nodes[node]; !ok {
		return nil
	}

	delete(c.Nodes, node)
	if err := op.Write(ctx, config.ConfigItemNodes, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) UpsertGroup(ctx context.Context, tenant, cluster, group string, body *GroupDTO) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	c, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return perrors.WithStack(err)
	}

	var ds *config.DataSourceCluster
	for i := range c.DataSourceClusters {
		if c.DataSourceClusters[i].Name == cluster {
			ds = c.DataSourceClusters[i]
			break
		}
	}

	if ds == nil {
		ds = &config.DataSourceCluster{
			Name: cluster,
			Type: config.DBMySQL,
		}
		_ = defaults.Set(ds)
		c.DataSourceClusters = append(c.DataSourceClusters, ds)
	}

	idx := slices.IndexFunc(ds.Groups, func(next *config.Group) bool {
		return next.Name == group
	})

	if idx == -1 {
		ds.Groups = append(ds.Groups, &config.Group{
			Name:  body.Name,
			Nodes: body.Nodes,
		})
	} else {
		ds.Groups[idx].Name = body.Name
		ds.Groups[idx].Nodes = body.Nodes
	}

	if err := op.Write(ctx, config.ConfigItemClusters, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) RemoveGroup(ctx context.Context, tenant, cluster, group string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	c, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return perrors.WithStack(err)
	}

	i := slices.IndexFunc(c.DataSourceClusters, func(next *config.DataSourceCluster) bool {
		return next.Name == cluster
	})
	if i == -1 {
		return perrors.Wrapf(errNoSuchCluster, "cannot remove group '%s'", group)
	}

	j := slices.IndexFunc(c.DataSourceClusters[i].Groups, func(next *config.Group) bool {
		return next.Name == group
	})

	if j == -1 {
		log.Warnf("omit removing non-exist group: tenant=%s, cluster=%s, group=%s", tenant, cluster, group)
		return nil
	}

	d := c.DataSourceClusters[i]

	copy(d.Groups[j:], d.Groups[j+1:])
	d.Groups[len(d.Groups)-1] = nil
	d.Groups = d.Groups[:len(d.Groups)-1]

	if err := op.Write(ctx, config.ConfigItemClusters, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) BindNode(ctx context.Context, tenant, cluster, group, node string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}
	c, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return perrors.WithStack(err)
	}

	i := slices.IndexFunc(c.DataSourceClusters, func(next *config.DataSourceCluster) bool {
		return next.Name == cluster
	})
	if i == -1 {
		return perrors.Wrapf(errNoSuchCluster, "cannot bind node %s::%s::%s::%s", tenant, cluster, group, node)
	}

	j := slices.IndexFunc(c.DataSourceClusters[i].Groups, func(next *config.Group) bool {
		return next.Name == group
	})
	if j == -1 {
		return perrors.Wrapf(errNoSuchGroup, "cannot bind node %s::%s::%s::%s", tenant, cluster, group, node)
	}

	g := c.DataSourceClusters[i].Groups[j]
	if slices.Contains(g.Nodes, node) {
		return nil
	}

	g.Nodes = append(g.Nodes, node)

	if err := op.Write(ctx, config.ConfigItemClusters, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) UnbindNode(ctx context.Context, tenant, cluster, group, node string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}
	c, err := op.Load(ctx, config.ConfigItemClusters)
	if err != nil {
		return perrors.WithStack(err)
	}

	i := slices.IndexFunc(c.DataSourceClusters, func(next *config.DataSourceCluster) bool {
		return next.Name == cluster
	})
	if i == -1 {
		return nil
	}

	j := slices.IndexFunc(c.DataSourceClusters[i].Groups, func(next *config.Group) bool {
		return next.Name == group
	})
	if j == -1 {
		return nil
	}

	g := c.DataSourceClusters[i].Groups[j]
	k := slices.Index(g.Nodes, node)
	if k == -1 {
		return nil
	}

	copy(g.Nodes[k:], g.Nodes[k+1:])
	g.Nodes[len(g.Nodes)-1] = ""
	g.Nodes = g.Nodes[:len(g.Nodes)-1]

	if err := op.Write(ctx, config.ConfigItemClusters, c); err != nil {
		return perrors.WithStack(err)
	}

	return nil
}

func (cs *MyConfigService) UpsertTable(ctx context.Context, tenant, cluster, table string, body *TableDTO) error {
	if body.Name == "" {
		body.Name = table
	}

	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	tenantCfg, err := op.Load(ctx, config.ConfigItemShardingRule)
	if err != nil {
		return err
	}

	var (
		rule      = tenantCfg.ShardingRule
		newTables = make([]*config.Table, len(rule.Tables))
		exist     = false
	)
	_ = reflect.Copy(reflect.ValueOf(newTables), reflect.ValueOf(rule.Tables))

	for _, tableCfg := range newTables {
		db, tb, err := misc.ParseTable(tableCfg.Name)
		if err != nil {
			return err
		}
		if db == cluster && tb == table {
			tableCfg.Sequence = body.Sequence
			tableCfg.DbRules = body.DbRules
			tableCfg.TblRules = body.TblRules
			tableCfg.Topology = body.Topology
			tableCfg.ShadowTopology = body.ShadowTopology
			tableCfg.Attributes = body.Attributes
			exist = true
			break
		}
	}
	if !exist {
		newTable := &config.Table{
			Name:           cluster + "." + table,
			Sequence:       body.Sequence,
			DbRules:        body.DbRules,
			TblRules:       body.TblRules,
			Topology:       body.Topology,
			ShadowTopology: body.ShadowTopology,
			Attributes:     body.Attributes,
		}
		newTables = append(newTables, newTable)
	}
	rule.Tables = newTables

	err = op.Write(ctx, config.ConfigItemShardingRule, tenantCfg)
	if err != nil {
		return err
	}
	return nil
}

func (cs *MyConfigService) RemoveTable(ctx context.Context, tenant, cluster, table string) error {
	op, err := cs.getCenter(ctx, tenant)
	if err != nil {
		return perrors.WithStack(err)
	}

	tenantCfg, err := op.Load(ctx, config.ConfigItemShardingRule)
	if err != nil {
		return perrors.WithStack(err)
	}

	if tenantCfg.ShardingRule == nil {
		return nil
	}

	var (
		rule           = tenantCfg.ShardingRule
		remainedTables = make([]*config.Table, 0, len(rule.Tables)-1)
	)

	for _, tableCfg := range rule.Tables {
		db, tb, err := misc.ParseTable(tableCfg.Name)
		if err != nil {
			return err
		}
		if db != cluster || tb != table {
			remainedTables = append(remainedTables, tableCfg)
		}
	}
	rule.Tables = remainedTables

	err = op.Write(ctx, config.ConfigItemShardingRule, tenantCfg)
	if err != nil {
		return err
	}
	return nil
}

func (cs *MyConfigService) getCenter(ctx context.Context, tenant string) (config.Center, error) {
	if exist, ok := cs.centers.Load(tenant); ok {
		return exist.(*lazyCenter).compute(ctx)
	}

	lc := &lazyCenter{
		loader: func(_ context.Context) (config.Center, error) {
			return config.NewCenter(
				tenant,
				config.GetStoreOperate(),
				config.WithReader(true),
				config.WithWriter(true),
			)
		},
	}

	actual, _ := cs.centers.LoadOrStore(tenant, lc)
	return actual.(*lazyCenter).compute(ctx)
}

type lazyCenter struct {
	sync.Mutex
	loader func(context.Context) (config.Center, error)
	cache  atomic.Value
}

func (la *lazyCenter) compute(ctx context.Context) (config.Center, error) {
	loaded, ok := la.cache.Load().(config.Center)
	if ok {
		return loaded, nil
	}

	la.Lock()
	defer la.Unlock()

	if loaded, ok = la.cache.Load().(config.Center); ok {
		return loaded, nil
	}

	value, err := la.loader(ctx)
	if err != nil {
		return nil, err
	}

	la.cache.Store(value)

	return value, nil
}
