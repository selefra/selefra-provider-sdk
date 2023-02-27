package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

func main() {
	exampleTable := &schema.Table{
		TableName: "example_parent_table",
		Options: &schema.TableOptions{
			PrimaryKeys: []string{
				"region",
				"name",
			},
		},
		Columns: []*schema.Column{
			{
				ColumnName: "region",
				Type:       schema.ColumnTypeString,
			},
			{
				ColumnName: "name",
				Type:       schema.ColumnTypeString,
			},
			{
				ColumnName: "id",
				Type:       schema.ColumnTypeString,
				// The value of this column is obtained from the two primary key columns above
				Extractor:  column_value_extractor.ColumnsValueMd5("region", "name"),
			},
		},
	}
	_ = exampleTable
}
