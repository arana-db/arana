package env

import (
	"os"
)

import (
	"github.com/arana-db/arana/pkg/constants"
)

// IsDevelopEnvironment check is the develop environment
func IsDevelopEnvironment() bool {
	dev := os.Getenv(constants.EnvDevelopEnvironment)
	if dev == "1" {
		return true
	}

	return false
}
