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
				// Gets the value of foo from ClientMeta
				Extractor: column_value_extractor.ClientMetaGetItem("foo"),
			},
			{
				ColumnName: "bar",
				Type:       schema.ColumnTypeString,
				// Gets the value of bar from ClientMeta, or returns the default value if bar does not exist
				Extractor: column_value_extractor.ClientMetaGetItemOrDefault("bar", "bar-value"),
			},
		},
	}
	_ = exampleTable
}
