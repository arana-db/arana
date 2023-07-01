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

package nacos

import (
	"strings"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/logger"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

import (
	u_conf "github.com/arana-db/arana/pkg/util/config"
)

const (
	_defaultPageSize = uint32(20)
)

type NacosServiceClient struct {
	naming_client.INamingClient

	pageSize    uint32
	NamespaceId string
	Servers     []string // ip+port, like 127.0.0.1:8848
}

func NewNacosServiceClient(options map[string]interface{}) (*NacosServiceClient, error) {
	nsc := &NacosServiceClient{}

	if val, ok := options[u_conf.NamespaceIdKey]; ok {
		nsc.NamespaceId = val.(string)
	}

	if val, ok := options[u_conf.Server]; ok {
		nsc.Servers = strings.Split(val.(string), u_conf.ServerSplit)
	}

	if val, ok := options[u_conf.PageSizeKey]; ok {
		nsc.pageSize = val.(uint32)
	} else {
		nsc.pageSize = _defaultPageSize
	}

	client, err := u_conf.NewNacosV2NamingClient(options)
	if err != nil {
		return nil, err
	}
	nsc.INamingClient = client

	return nsc, nil
}

func (ngsc *NacosServiceClient) SelectAllServiceInstances() []model.Instance {
	var (
		result       = make([]model.Instance, 0)
		count        int
		currentCount = 0
		pageNo       = uint32(1)
		pageSize     = ngsc.pageSize
	)

	srvs, err := ngsc.GetAllServicesInfo(vo.GetAllServiceInfoParam{PageNo: pageNo, PageSize: 1})
	if err != nil {
		logger.Warnf("Failed to service count from nacos", err)
		return nil
	}

	count = int(srvs.Count)
	for currentCount < count {
		srvs, err = ngsc.GetAllServicesInfo(vo.GetAllServiceInfoParam{PageNo: pageNo, PageSize: pageSize})
		if err != nil {
			logger.Warnf("Failed to get all services info from nacos: %s with pageNo:%d and pageSize:%d", err, pageNo, pageSize)
			return nil
		}
		for _, srv := range srvs.Doms {
			instances, err := ngsc.SelectAllInstances(vo.SelectAllInstancesParam{ServiceName: srv})
			if err != nil {
				logger.Warnf("Failed to get all instances of service %s info from nacos: %s", srv, err)
				return nil
			}
			logger.Infof("Successfully get all instances of service %s\n: ", srv, instances)
			result = append(result, instances...)
		}
		pageNo++
		currentCount += len(srvs.Doms)
	}

	return result
}
