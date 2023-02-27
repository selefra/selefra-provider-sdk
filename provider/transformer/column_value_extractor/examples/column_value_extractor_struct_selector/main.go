package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

type RawResult struct {
	Foo Foo
}

type Foo struct {
	Bar Bar
}

type Bar struct {
	Name string
}

func main() {
	exampleTable := &schema.Table{
		TableName: "example_table",
		Columns: []*schema.Column{
			{
				ColumnName: "name",
				Type:       schema.ColumnTypeString,
				Extractor:  column_value_extractor.StructSelector("Foo.Bar.Name"),
			},
		},
	}
	_ = exampleTable
}
