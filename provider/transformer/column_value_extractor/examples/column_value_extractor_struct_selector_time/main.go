package main

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"time"
)

type RawResult struct {
	Foo Foo
}

type Foo struct {
	Bar Bar
}

type Bar struct {
	Date time.Time
}

func main() {
	exampleTable := &schema.Table{
		TableName: "example_table",
		Columns: []*schema.Column{
			{
				ColumnName: "date",
				Type:       schema.ColumnTypeTimestamp,
				Extractor:  column_value_extractor.StructSelectorTime("Foo.Bar.Date", "2006-01-02 15:04:05"),
			},
		},
	}
	_ = exampleTable
}
