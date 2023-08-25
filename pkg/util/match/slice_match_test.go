package match

import (
	"reflect"
	"testing"
)

func TestIsEmpty(t *testing.T) {
	// Test nil case
	if !isEmpty(nil) {
		t.Errorf("Expected true for nil case")
	}

	// Test empty array
	arr := []int{}
	if !isEmpty(arr) {
		t.Errorf("Expected true for empty array")
	}

	// Test non-empty array
	arr = []int{1, 2, 3}
	if isEmpty(arr) {
		t.Errorf("Expected false for non-empty array")
	}

	// Test empty map
	m := map[string]int{}
	if !isEmpty(m) {
		t.Errorf("Expected true for empty map")
	}

	// Test non-empty map
	m = map[string]int{"a": 1}
	if isEmpty(m) {
		t.Errorf("Expected false for non-empty map")
	}

	// Test empty slice
	s := make([]int, 0)
	if !isEmpty(s) {
		t.Errorf("Expected true for empty slice")
	}

	// Test non-empty slice
	s = make([]int, 3)
	if isEmpty(s) {
		t.Errorf("Expected false for non-empty slice")
	}

	// Test empty channel
	ch := make(chan int)
	if !isEmpty(ch) {
		t.Errorf("Expected true for empty channel")
	}

	// Test non-empty channel
	// ch <- 1
	// if isEmpty(ch) {
	// 	 t.Errorf("Expected false for non-empty channel")
	// }

	// Test empty pointer
	var ptr *int
	if !isEmpty(ptr) {
		t.Errorf("Expected true for empty pointer")
	}

	// Test non-empty pointer
	i := 10
	ptr = &i
	if isEmpty(ptr) {
		t.Errorf("Expected false for non-empty pointer")
	}

	// Test zero value
	str := ""
	if !isEmpty(str) {
		t.Errorf("Expected true for zero value")
	}

	// Test non-zero value
	str = "Hello"
	if isEmpty(str) {
		t.Errorf("Expected false for non-zero value")
	}
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

// 测试 expected 和 actual 都为 nil 的情况
func TestObjectsAreEqual_BothNil(t *testing.T) {
	if !ObjectsAreEqual(nil, nil) {
		t.Error("Expected ObjectsAreEqual(nil, nil) to return true")
	}
}

// 测试 expected 为 nil，actual 不为 nil 的情况
func TestObjectsAreEqual_ExpectedNil(t *testing.T) {
	if ObjectsAreEqual(nil, "some value") {
		t.Error("Expected ObjectsAreEqual(nil, \"some value\") to return false")
	}
}

// 测试 expected 不为 nil，actual 为 nil 的情况
func TestObjectsAreEqual_ActualNil(t *testing.T) {
	if ObjectsAreEqual("some value", nil) {
		t.Error("Expected ObjectsAreEqual(\"some value\", nil) to return false")
	}
}

// 测试 expected 和 actual 都为 []byte 类型，且内容相等的情况
func TestObjectsAreEqual_BothBytesEqual(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := []byte{1, 2, 3}
	if !ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], [1, 2, 3]) to return true")
	}
}

// 测试 expected 和 actual 都为 []byte 类型，但内容不相等的情况
func TestObjectsAreEqual_BothBytesNotEqual(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := []byte{4, 5, 6}
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], [4, 5, 6]) to return false")
	}
}

// 测试 expected 不为 []byte 类型，actual 为 []byte 类型的情况
func TestObjectsAreEqual_ExpectedNotBytes(t *testing.T) {
	expected := "some value"
	actual := []byte{1, 2, 3}
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", [1, 2, 3]) to return false")
	}
}

// 测试 expected 为 []byte 类型，actual 不为 []byte 类型的情况
func TestObjectsAreEqual_ActualNotBytes(t *testing.T) {
	expected := []byte{1, 2, 3}
	actual := "some value"
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual([1, 2, 3], \"some value\") to return false")
	}
}

// 测试 expected 和 actual 都为非 nil 且非 []byte 类型，且内容相等的情况
func TestObjectsAreEqual_BothNonBytesEqual(t *testing.T) {
	expected := "some value"
	actual := "some value"
	if !ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", \"some value\") to return true")
	}
}

// 测试 expected 和 actual 都为非 nil 且非 []byte 类型，但内容不相等的情况
func TestObjectsAreEqual_BothNonBytesNotEqual(t *testing.T) {
	expected := "some value"
	actual := "another value"
	if ObjectsAreEqual(expected, actual) {
		t.Error("Expected ObjectsAreEqual(\"some value\", \"another value\") to return false")
	}
}
