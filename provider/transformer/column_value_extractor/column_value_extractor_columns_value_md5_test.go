package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorColumnsValueMd5(t *testing.T) {

	row := schema.NewRow("name", "age", "sex")
	_ = row.SetValues([]any{
		"Tom",
		3,
		"boy",
	})

	value, diagnostics := ColumnsValueMd5("name", "age").Extract(context.Background(), nil, nil, &schema.DataSourcePullTask{
		Table: &schema.Table{},
	}, row, &schema.Column{}, nil)

	assert.Equal(t, diagnostics == nil || !diagnostics.HasError(), true)
	assert.Equal(t, "b725db465095b4f713647240e78a668d", value)

}
