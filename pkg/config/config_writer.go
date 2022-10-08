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
 *
 */

package config

import (
	"context"
	"encoding/json"
)

import (
	"github.com/pkg/errors"

	"github.com/tidwall/gjson"
)

type configWriter struct {
	tenant string

	storeOperate StoreOperator
	pathInfo     *PathInfo
}

func (c *configWriter) Close() error {
	return nil
}

func (c *configWriter) Write(ctx context.Context, item ConfigItem, cfg *Tenant) error {
	allKeyMap := c.pathInfo.ConfigKeyMapping
	ret := make(map[PathKey]string)

	for i := range allKeyMap {
		if allKeyMap[i] == string(item) {
			ret[i] = allKeyMap[i]
		}
	}

	return c.doPersist(ctx, cfg, ret)
}

func (c *configWriter) Import(ctx context.Context, cfg *Tenant) error {
	return c.doPersist(ctx, cfg, c.pathInfo.ConfigKeyMapping)
}

func (c *configWriter) doPersist(ctx context.Context, conf *Tenant, keyMap map[PathKey]string) error {

	configJson, err := json.Marshal(conf)
	if err != nil {
		return errors.Wrap(err, "config json.marshal failed")
	}

	for k, v := range keyMap {

		ret, err := JSONToYAML(gjson.GetBytes(configJson, v).String())
		if err != nil {
			return err
		}

		if err := c.storeOperate.Save(k, ret); err != nil {
			return err
		}
	}
	return nil
}
