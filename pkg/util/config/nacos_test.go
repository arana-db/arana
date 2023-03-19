package config

import (
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func Test_parseServerConfig(t *testing.T) {
	// NamespaceIdKey string = "namespace"
	// GroupKey     string = "group"
	// Username     string = "username"
	// Password     string = "password"
	// Server       string = "endpoints"
	// ContextPath  string = "contextPath"
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
