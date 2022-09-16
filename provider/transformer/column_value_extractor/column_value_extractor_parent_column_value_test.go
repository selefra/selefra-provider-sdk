package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorParentColumnValue(t *testing.T) {

	parentRow := schema.NewRow("name", "age")
	_ = parentRow.SetValues([]any{
		"Tom",
		3,
	})

	value, diagnostics := ParentColumnValue("name").Extract(context.Background(), nil, nil, &schema.DataSourcePullTask{
		ParentRow: parentRow,
	}, nil, nil, nil)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, value, "Tom")

}
