/*
 * //
 * // Licensed to Apache Software Foundation (ASF) under one or more contributor
 * // license agreements. See the NOTICE file distributed with
 * // this work for additional information regarding copyright
 * // ownership. Apache Software Foundation (ASF) licenses this file to you under
 * // the Apache License, Version 2.0 (the "License"); you may
 * // not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing,
 * // software distributed under the License is distributed on an
 * // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * // KIND, either express or implied.  See the License for the
 * // specific language governing permissions and limitations
 * // under the License.
 * //
 */

package etcd

import (
	"encoding/json"
	"github.com/arana-db/arana/pkg/config"
	"time"
)

import (
	etcdv3 "github.com/dubbogo/gost/database/kv/etcd/v3"

	"github.com/pkg/errors"

	"github.com/tidwall/gjson"
)

type Client struct {
	client *etcdv3.Client
}

func NewClient(endpoint []string) (*Client, error) {
	tmpClient, err := etcdv3.NewConfigClientWithErr(
		etcdv3.WithName(etcdv3.RegistryETCDV3Client),
		etcdv3.WithTimeout(10*time.Second),
		etcdv3.WithEndpoints(endpoint...),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize etcd client")
	}
	return &Client{client: tmpClient}, nil
}

// PutConfigToEtcd initialize local file config into etcd，only be used in when etcd don't hava data.
func (c *Client) PutConfigToEtcd(configPath string) error {
	conf, err := config.LoadV2(configPath)
	if err != nil {
		return errors.WithStack(err)
	}

	configJson, err := json.Marshal(conf)
	if err != nil {
		return errors.Errorf("config json.marshal failed  %v err:", err)
	}

	if err = c.client.Put(config.DefaultConfigPath, string(configJson)); err != nil {
		return err
	}

	if err = c.client.Put(config.DefaultConfigDataListenersPath, gjson.Get(string(configJson), "data.listeners").String()); err != nil {
		return err
	}

	if err = c.client.Put(config.DefaultConfigDataExecutorsPath, gjson.Get(string(configJson), "data.executors").String()); err != nil {
		return err
	}

	if err = c.client.Put(config.DefaultConfigDataSourceClustersPath, gjson.Get(string(configJson), "data.dataSourceClusters").String()); err != nil {
		return err
	}

	if err = c.client.Put(config.DefaultConfigDataShardingRulePath, gjson.Get(string(configJson), "data.shardingRule").String()); err != nil {
		return err
	}

	return nil
}

// LoadConfigFromEtcd get key value from etcd
func (c *Client) LoadConfigFromEtcd(configKey string) (string, error) {
	resp, err := c.client.Get(configKey)
	if err != nil {
		return "", errors.Errorf("Get remote config fail error %v", err)
	}
	return resp, nil
}

// UpdateConfigToEtcd update key value in etcd
func (c *Client) UpdateConfigToEtcd(configKey, configValue string) error {

	if err := c.client.Put(configKey, configValue); err != nil {
		return err
	}

	return nil
}
