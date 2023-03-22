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

package config

import (
	"strconv"
	"strings"
)

import (
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
)

const (
	DefaultGroupName string = "arana"

	NamespaceIdKey string = "namespace-id"
	GroupKey       string = "group"
	Username       string = "username"
	Password       string = "password"
	Server         string = "endpoints"
	ContextPath    string = "context-path"
	Scheme         string = "scheme"
	PageSizeKey    string = "page-size"

	PathSplit   string = "::"
	ServerSplit string = ","
)

func NewNacosV2NamingClient(options map[string]interface{}) (naming_client.INamingClient, error) {
	properties := make(map[string]interface{})
	properties[constant.KEY_CLIENT_CONFIG] = ParseNacosClientConfig(options)
	properties[constant.KEY_SERVER_CONFIGS] = ParseNacosServerConfig(options)
	return clients.CreateNamingClient(properties)
}

func ParseNacosServerConfig(options map[string]interface{}) []constant.ServerConfig {
	cfgs := make([]constant.ServerConfig, 0)

	scheme := "http"
	if val, ok := options[Scheme]; ok {
		scheme = val.(string)
	}
	contextPath := "/nacos"
	if val, ok := options[ContextPath]; ok {
		contextPath = val.(string)
	}

	if servers, ok := options[Server]; ok {
		addresses := strings.Split(servers.(string), ServerSplit)
		for i := range addresses {
			addr := strings.Split(strings.TrimSpace(addresses[i]), ":")

			ip := addr[0]
			port, _ := strconv.ParseInt(addr[1], 10, 64)

			cfgs = append(cfgs, constant.ServerConfig{
				Scheme:      scheme,
				ContextPath: contextPath,
				IpAddr:      ip,
				Port:        uint64(port),
			})
		}
	}

	return cfgs
}

func ParseNacosClientConfig(options map[string]interface{}) constant.ClientConfig {
	cc := constant.ClientConfig{}

	if val, ok := options[NamespaceIdKey]; ok {
		cc.NamespaceId = val.(string)
	}
	if val, ok := options[Username]; ok {
		cc.Username = val.(string)
	}
	if val, ok := options[Password]; ok {
		cc.Password = val.(string)
	}
	return cc
}
