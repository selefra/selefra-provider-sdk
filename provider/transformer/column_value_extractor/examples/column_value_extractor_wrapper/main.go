package main

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
)

type RawResult struct {
	N int
}

func main() {
	exampleTable := &schema.Table{
		TableName: "example_table",
		Columns: []*schema.Column{
			{
				ColumnName: "n_plus",
				Type:       schema.ColumnTypeInt,
				Extractor: column_value_extractor.WrapperExtractFunction(func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
					r := result.(*RawResult)
					return r.N + 1, nil
				}),
			},
		},
	}
	_ = exampleTable
}
