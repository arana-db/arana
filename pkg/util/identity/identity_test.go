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
