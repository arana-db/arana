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
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func Test_parseServerConfig(t *testing.T) {
	// NamespaceIdKey string = "namespace-id"
	// GroupKey     string = "group"
	// Username     string = "username"
	// Password     string = "password"
	// Server       string = "endpoints"
	// ContextPath  string = "context-path"
	// Scheme       string = "scheme"

	options := map[string]interface{}{
		NamespaceIdKey: "arana_test",
		GroupKey:       "arana_test",
		Username:       "nacos_test",
		Password:       "nacos_test",
		Server:         "127.0.0.1:8848,127.0.0.2:8848",
	}

	clientConfig := ParseNacosClientConfig(options)
	assert.Equal(t, options[NamespaceIdKey], clientConfig.NamespaceId)
	assert.Equal(t, options[Username], clientConfig.Username)
	assert.Equal(t, options[Password], clientConfig.Password)

	serverConfigs := ParseNacosServerConfig(options)
	assert.Equal(t, 2, len(serverConfigs))

	assert.Equal(t, "127.0.0.1", serverConfigs[0].IpAddr)
	assert.Equal(t, "127.0.0.2", serverConfigs[1].IpAddr)
}
