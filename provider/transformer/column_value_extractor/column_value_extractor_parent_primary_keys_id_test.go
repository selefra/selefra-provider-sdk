package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorParentPrimaryKeysID(t *testing.T) {

	parentTable := &schema.Table{
		Options: &schema.TableOptions{
			PrimaryKeys: []string{
				"country",
				"name",
			},
		},
	}

	parentRow := schema.NewRow("country", "name")
	_ = parentRow.SetValues([]any{
		"China",
		"Tom",
	})

	value, diagnostics := ParentPrimaryKeysID().Extract(context.Background(), nil, nil, &schema.DataSourcePullTask{
		ParentTable: parentTable,
		ParentRow:   parentRow,
	}, nil, nil, nil)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, "4474bfd8854322bd7c5d347b016f4e23", value)
}
