package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

type ParentRawResult struct {
	Foo string
}

func main() {
	exampleTable := &schema.Table{
		TableName: "example_parent_table",
		Columns:   []*schema.Column{
			// ...
		},
		SubTables: []*schema.Table{
			{
				TableName: "example_child_table",
				Columns: []*schema.Column{
					{
						ColumnName: "foo",
						Type:       schema.ColumnTypeString,
						Extractor:  column_value_extractor.ParentResultStructSelector("foo"),
					},
				},
			},
		},
	}
	_ = exampleTable
}
