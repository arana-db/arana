// Package testdata 测试资源包
package testdata

import (
	"path/filepath"
	"runtime"

	_ "github.com/golang/mock/gomock"
)

var basepath string

func init() {
	_, currentFile, _, _ := runtime.Caller(0)
	basepath = filepath.Dir(currentFile)
}

func Path(rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}
	return filepath.Join(basepath, rel)
}
