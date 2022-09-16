package schema

import (
	"github.com/selefra/selefra-utils/pkg/id_util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultClientLogger_New(t *testing.T) {

	workspace := "./"
	providerName := "test"
	clientLogger, err := NewDefaultClientLogger(NewDefaultClientLoggerConfig(workspace, providerName))
	assert.Nil(t, err)
	clientLogger.Info("test")

}

func TestDefaultClientLogger_Rotate(t *testing.T) {
	workspace := "./"
	providerName := "test"
	clientLogger, err := NewDefaultClientLogger(NewDefaultClientLoggerConfig(workspace, providerName))
	assert.Nil(t, err)

	for i := 1; i <= 10000000; i++ {
		clientLogger.Debug(id_util.RandomId())
		clientLogger.Info(id_util.RandomId())
		clientLogger.Warn(id_util.RandomId())
		clientLogger.Error(id_util.RandomId())
	}
}

func Test_logDirectory(t *testing.T) {
}
