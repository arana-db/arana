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

package match

import (
	"reflect"
	"testing"
)

import (
	"github.com/stretchr/testify/assert"
)

func TestIsEmpty(t *testing.T) {
	// Test nil case
	assert.True(t, isEmpty(nil))

	// Test empty array
	arr := []int{}
	assert.True(t, isEmpty(arr))

	// Test non-empty array
	arr = []int{1, 2, 3}
	assert.False(t, isEmpty(arr))

	// Test empty map
	m := map[string]int{}
	assert.True(t, isEmpty(m))

	// Test non-empty map
	m = map[string]int{"a": 1}
	assert.False(t, isEmpty(m))

	// Test empty slice
	s := make([]int, 0)
	assert.True(t, isEmpty(s))

	// Test non-empty slice
	s = make([]int, 3)
	assert.False(t, isEmpty(s))

	// Test empty channel
	ch := make(chan int)
	assert.True(t, isEmpty(ch))

	// Test non-empty channel
	// ch <- 1
	// if isEmpty(ch) {
	// 	 t.Errorf("Expected false for non-empty channel")
	// }

	// Test empty pointer
	var ptr *int
	assert.True(t, isEmpty(ptr))

	// Test non-empty pointer
	i := 10
	ptr = &i
	assert.False(t, isEmpty(ptr))

	// Test zero value
	str := ""
	assert.True(t, isEmpty(str))

	// Test non-zero value
	str = "Hello"
	assert.False(t, isEmpty(s))
}

func TestElementsMatch(t *testing.T) {
	// Test case 1: Both lists are empty
	if !ElementsMatch([]int{}, []int{}) {
		t.Error("ElementsMatch([]int{}, []int{}) should return true")
	}

	// Test case 2: One of the lists is empty
	if ElementsMatch([]int{}, []int{1, 2, 3}) {
		t.Error("ElementsMatch([]int{}, []int{1, 2, 3}) should return false")
	}

	// Test case 3: Lists contain different types of elements
	if ElementsMatch([]int{1, 2, 3}, []string{"1", "2", "3"}) {
		t.Error("ElementsMatch([]int{1, 2, 3}, []string{\"1\", \"2\", \"3\"}) should return false")
	}

	// Test case 4: Lists contain the same elements
	if !ElementsMatch([]int{1, 2, 3}, []int{3, 2, 1}) {
		t.Error("ElementsMatch([]int{1, 2, 3}, []int{3, 2, 1}) should return true")
	}

	// Test case 5: Lists contain extra elements
	if ElementsMatch([]int{1, 2, 3}, []int{1, 2, 3, 4}) {
		t.Error("ElementsMatch([]int{1, 2, 3}, []int{1, 2, 3, 4}) should return false")
	}

	// Test case 6: Lists contain missing elements
	if ElementsMatch([]int{1, 2, 3, 4}, []int{1, 2, 3}) {
		t.Error("ElementsMatch([]int{1, 2, 3, 4}, []int{1, 2, 3}) should return false")
	}
}

func TestIsList(t *testing.T) {
	// Test case 1: Test with an array
	arr := [3]int{1, 2, 3}
	if !isList(arr) {
		t.Error("Expected true for array, got false")
	}

	// Test case 2: Test with a slice
	slice := []int{4, 5, 6}
	if !isList(slice) {
		t.Error("Expected true for slice, got false")
	}

	// Test case 3: Test with a map
	m := make(map[string]int)
	if isList(m) {
		t.Error("Expected false for map, got true")
	}

	// Test case 4: Test with a string
	str := "hello"
	if isList(str) {
		t.Error("Expected false for string, got true")
	}

	// Test case 5: Test with an integer
	num := 10
	if isList(num) {
		t.Error("Expected false for integer, got true")
	}
}

func TestDiffLists(t *testing.T) {
	// Test case 1: Both lists are empty
	listA := []interface{}{}
	listB := []interface{}{}
	extraA, extraB := diffLists(listA, listB)
	if len(extraA) != 0 || len(extraB) != 0 {
		t.Errorf("Expected no extra elements, got extraA=%v, extraB=%v", extraA, extraB)
	}

	// Test case 2: listA is empty, listB has elements
	listA = []interface{}{}
	listB = []interface{}{1, 2, 3}
	extraA, extraB = diffLists(listA, listB)
	if len(extraA) != 0 || !reflect.DeepEqual(extraB, listB) {
		t.Errorf("Expected no extra elements in listA and extraB=%v, got extraA=%v, extraB=%v", listB, extraA, extraB)
	}

	// Test case 3: listB is empty, listA has elements
	listA = []interface{}{1, 2, 3}
	listB = []interface{}{}
	extraA, extraB = diffLists(listA, listB)
	if !reflect.DeepEqual(extraA, listA) || len(extraB) != 0 {
		t.Errorf("Expected extraA=%v and no extra elements in listB, got extraA=%v, extraB=%v", listA, extraA, extraB)
	}

	// Test case 4: Both lists have elements, but no common elements
	listA = []interface{}{1, 2, 3}
	listB = []interface{}{4, 5, 6}
	extraA, extraB = diffLists(listA, listB)
	if !reflect.DeepEqual(extraA, listA) || !reflect.DeepEqual(extraB, listB) {
		t.Errorf("Expected extraA=%v and extraB=%v, got extraA=%v, extraB=%v", listA, listB, extraA, extraB)
	}

	// Test case 5: Both lists have elements, with some common elements
	listA = []interface{}{1, 2, 3}
	listB = []interface{}{2, 3, 4}
	extraA, extraB = diffLists(listA, listB)
	expectedExtraA := []interface{}{1}
	expectedExtraB := []interface{}{4}
	if !reflect.DeepEqual(extraA, expectedExtraA) || !reflect.DeepEqual(extraB, expectedExtraB) {
		t.Errorf("Expected extraA=%v and extraB=%v, got extraA=%v, extraB=%v", expectedExtraA, expectedExtraB, extraA, extraB)
	}
}

// Test case 1: expected and actual are nil
func TestObjectsAreEqual_BothNil(t *testing.T) {
	if !ObjectsAreEqual(nil, nil) {
		t.Error("Expected ObjectsAreEqual(nil, nil) to return true")
	}
}

// Test case 2: expected is nil，actual is not nil
func TestObjectsAreEqual_ExpectedNil(t *testing.T) {
	if ObjectsAreEqual(nil, "some value") {
		t.Error("Expected ObjectsAreEqual(nil, \"some value\") to return false")
	}
}

// Test case 3: expected is not nil，actual is nil
func TestObjectsAreEqual_ActualNil(t *testing.T) {
	if ObjectsAreEqual("some value", nil) {
		t.Error("Expected ObjectsAreEqual(\"some value\", nil) to return false")
	}
}

// Test case 4: expected and actual are the type of []byte, and the contents are equal
func TestObjectsAreEqual_BothBytesEqual(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := []byte{1, 2, 3}
	if !ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], [1, 2, 3]) to return true")
	}
}

// Test case 5: expected and actual are the type of []byte, and the contents are not equal
func TestObjectsAreEqual_BothBytesNotEqual(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := []byte{4, 5, 6}
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], [4, 5, 6]) to return false")
	}
}

// Test case 6: expected is not the type of []byte, but actual is the type of []byte
func TestObjectsAreEqual_ExpectedNotBytes(t *testing.T) {
	expected := "some value"
	actual := []byte{1, 2, 3}
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", [1, 2, 3]) to return false")
	}
}

// Test case 7: expected is the type of []byte, but actual is not the type []byte
func TestObjectsAreEqual_ActualNotBytes(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := "some value"
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], \"some value\") to return false")
	}
}

// Test case 7: expected and actual are not nil, and not the type of []byte, but the contents are equal
func TestObjectsAreEqual_BothNonBytesEqual(t *testing.T) {
	expected := "some value"
	actual := "some value"
	if !ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", \"some value\") to return true")
	}
}

// Test case 8: expected and actual are not nil, and not the type of []byte, but the contents are not equal
func TestObjectsAreEqual_BothNonBytesNotEqual(t *testing.T) {
	expected := "some value"
	actual := "another value"
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", \"another value\") to return false")
	}
}
