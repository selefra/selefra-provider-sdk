package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

type ColumnValueExtractorConstant struct {
	value any
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorConstant{}

func (x *ColumnValueExtractorConstant) Name() string {
	return "constant-column-value-extractor"
}

func (x *ColumnValueExtractorConstant) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	return x.value, nil
}

func (x *ColumnValueExtractorConstant) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorConstant) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

func Constant(value any) *ColumnValueExtractorConstant {
	return &ColumnValueExtractorConstant{
		value: value,
	}
}
