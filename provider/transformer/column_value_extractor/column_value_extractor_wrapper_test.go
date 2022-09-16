package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorWrapper(t *testing.T) {

	extractor := Wrapper("foo", func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
		return "foo", nil
	}, nil, nil)

	assert.Equal(t, "foo", extractor.Name())

	result, err := extractor.Extract(context.Background(), nil, nil, nil, nil, nil, nil)

	assert.Nil(t, err)
	assert.Equal(t, result, "foo")
}
