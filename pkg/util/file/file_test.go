package file

import (
	"testing"
)

func TestIsYaml(t *testing.T) {
	// Test with a valid YAML file
	path := "example.yaml"
	expected := true
	result := IsYaml(path)
	if result != expected {
		t.Errorf("IsYaml(%s) = %v; want %v", path, result, expected)
	}

	// Test with a valid YML file
	path = "example.yml"
	expected = true
	result = IsYaml(path)
	if result != expected {
		t.Errorf("IsYaml(%s) = %v; want %v", path, result, expected)
	}

	// Test with an invalid file extension
	path = "example.txt"
	expected = false
	result = IsYaml(path)
	if result != expected {
		t.Errorf("IsYaml(%s) = %v; want %v", path, result, expected)
	}
}
