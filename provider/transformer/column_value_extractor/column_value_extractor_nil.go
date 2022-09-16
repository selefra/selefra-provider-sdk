package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

type ColumnValueExtractorNil struct {
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorNil{}

func (x *ColumnValueExtractorNil) Name() string {
	return "nil-column-value-extractor"
}

func (x *ColumnValueExtractorNil) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	return nil, nil
}

func (x *ColumnValueExtractorNil) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorNil) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

func Nil() *ColumnValueExtractorNil {
	return &ColumnValueExtractorNil{}
}
