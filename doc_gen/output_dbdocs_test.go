package doc_gen

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewDBDocsGenerator(t *testing.T) {
	provider := getTestProvider()
	result, err := GeneratorDBDocsWithPostgresqlDSNEnv(provider)
	assert.Nil(t, err)
	t.Log(result)
}
