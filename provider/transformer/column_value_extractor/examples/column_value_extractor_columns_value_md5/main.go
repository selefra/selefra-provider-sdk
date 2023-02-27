package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

func main() {
	exampleTable := &schema.Table{
		TableName: "example",
		Options: &schema.TableOptions{
			PrimaryKeys: []string{"id"},
		},
		Columns: []*schema.Column{
			{
				ColumnName: "id",
				Type:       schema.ColumnTypeString,
				// The values of the region and name columns are concatenated and the MD5 value is used as the value of the current column
				Extractor: column_value_extractor.ColumnsValueMd5("region", "name"),
			},
		},
	}
	_ = exampleTable
}
