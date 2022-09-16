package column_value_extractor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorConstant(t *testing.T) {

	value, diagnostics := Constant("Tom").Extract(context.Background(), nil, nil, nil, nil, nil, nil)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, value, "Tom")

}
