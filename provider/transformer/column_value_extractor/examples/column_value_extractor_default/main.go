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
				ColumnName: "region_name",
				Type:       schema.ColumnTypeString,
				// If you use the default extractor, you don't need to write it out, and if you omit it, it's this extractor
				Extractor: column_value_extractor.Default(),
			},
		},
	}
	_ = exampleTable
}
