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

package identity

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestGetNodeIdentity(t *testing.T) {
	// Test case 1: Testing when AranaNodeId is set
	expectedNodeId := "some-node-id"
	os.Setenv("ARANA_NODE_ID", expectedNodeId)
	assert.Equal(t, expectedNodeId, GetNodeIdentity())
	os.Unsetenv("ARANA_NODE_ID")

	// Test case 2: Testing when PodName is set
	expectedPodName := "some-pod-name"
	os.Setenv("POD_NAME", expectedPodName)
	assert.Equal(t, expectedPodName, GetNodeIdentity())
	os.Unsetenv("POD_NAME")

	// Test case 3: Testing when neither AranaNodeId nor PodName is set
	//expectedIP := "some-ip"
	//net.FindSelfIP = func() (string, error) {
	//	return expectedIP, nil
	//}
	//assert.Equal(t, expectedIP, GetNodeIdentity())
	//net.FindSelfIP = nil

	// Test case 4: Testing when all values are empty
	//expectedUUID := "some-uuid"
	//uuid.NewString = func() string {
	//	return expectedUUID
	//}
	//assert.Equal(t, expectedUUID, GetNodeIdentity())
	//uuid.NewString = nil
}
