package column_value_extractor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorNil(t *testing.T) {

	value, diagnostics := Nil().Extract(context.Background(), nil, nil, nil, nil, nil, nil)

	assert.Nil(t, diagnostics)
	assert.Nil(t, value)

}
