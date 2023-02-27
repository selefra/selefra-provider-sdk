package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

func main() {

	exampleTable := &schema.Table{
		TableName: "example_parent_table",
		Columns: []*schema.Column{
			{
				ColumnName: "id",
				Type:       schema.ColumnTypeString,
				Extractor:  column_value_extractor.UUID(),
			},
		},
		SubTables: []*schema.Table{
			{
				TableName: "example_child_table",
				Columns: []*schema.Column{
					{
						ColumnName: "parent_id",
						Type:       schema.ColumnTypeString,
						// The parent table is associated with the value of this column, which is equivalent to a foreign key column
						Extractor: column_value_extractor.ParentColumnValue("id"),
					},
				},
			},
		},
	}
	_ = exampleTable
}
