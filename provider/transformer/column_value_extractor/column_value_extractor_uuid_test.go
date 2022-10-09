package column_value_extractor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"regexp"
	"testing"
)

func TestColumnValueExtractorUUID(t *testing.T) {

	// default
	value, diagnostics := UUID().Extract(context.Background(), nil, nil, nil, nil, nil, nil)
	assert.Nil(t, diagnostics)
	assert.True(t, regexp.MustCompile("^[0-9a-fA-F]{32}$").Match([]byte(value.(string))))

	// use horizontal
	value, diagnostics = UUID(false).Extract(context.Background(), nil, nil, nil, nil, nil, nil)
	assert.Nil(t, diagnostics)
	assert.True(t, regexp.MustCompile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$").Match([]byte(value.(string))))

}
