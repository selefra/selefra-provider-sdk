package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/songzhibin97/go-ognl"
)

type ColumnValueExtractorStructSelector struct {
	selectors []string
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorStructSelector{}

func (x *ColumnValueExtractorStructSelector) Name() string {
	return "struct-selector-column-value-extractor"
}

func (x *ColumnValueExtractorStructSelector) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	for _, selector := range x.selectors {
		v := ognl.Get(result, selector).Value()
		if !reflect_util.IsNil(v) {
			return v, nil
		}
	}
	return nil, nil
}

func (x *ColumnValueExtractorStructSelector) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorStructSelector) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

// StructSelector Extract the value based on the struct selector expression passed in
func StructSelector(structSelector ...string) *ColumnValueExtractorStructSelector {
	return &ColumnValueExtractorStructSelector{
		selectors: structSelector,
	}
}
