package runes

import "fmt"

func ConvertToRune(value interface{}) []rune {
	return []rune(fmt.Sprint(value))
}
