package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorPrimaryKeysID(t *testing.T) {
	table := &schema.Table{
		Options: &schema.TableOptions{
			PrimaryKeys: []string{
				"country",
				"name",
			},
		},
	}

	row := schema.NewRow("country", "name")
	_ = row.SetValues([]any{
		"China",
		"Tom",
	})

	value, diagnostics := PrimaryKeysID().Extract(context.Background(), nil, nil, &schema.DataSourcePullTask{
		Table: table,
	}, row, nil, nil)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, "4474bfd8854322bd7c5d347b016f4e23", value)
}
