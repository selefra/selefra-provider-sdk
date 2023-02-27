package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

func main() {
	exampleTable := &schema.Table{
		TableName: "example",
		Columns: []*schema.Column{
			{
				ColumnName: "region",
				Type:       schema.ColumnTypeString,
				// Use a fixed value as the value of this column, that is, the value of this column is write-dead
				Extractor: column_value_extractor.Constant("north-01"),
			},
		},
	}
	_ = exampleTable
}
