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
				Extractor:  column_value_extractor.ColumnsValueMd5("region", "name"),
			},
		},
		SubTables: []*schema.Table{
			{
				TableName: "example_child_table",
				Columns: []*schema.Column{
					{
						ColumnName: "parent_pk_id",
						Type:       schema.ColumnTypeString,
						Extractor:  column_value_extractor.ParentPrimaryKeysID(),
					},
				},
			},
		},
	}
	_ = exampleTable
}
