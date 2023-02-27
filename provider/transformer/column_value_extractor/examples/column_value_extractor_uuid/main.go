package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

func main() {
	exampleTable := &schema.Table{
		TableName: "example_table",
		Columns: []*schema.Column{
			{
				ColumnName: "id",
				Type:       schema.ColumnTypeString,
				Extractor:  column_value_extractor.UUID(),
			},
		},
	}
	_ = exampleTable
}
