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
				ColumnName: "foo",
				Type:       schema.ColumnTypeString,
				// The value of this column is always null
				Extractor: column_value_extractor.Nil(),
			},
		},
	}
	_ = exampleTable
}
