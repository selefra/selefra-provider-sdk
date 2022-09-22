package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/songzhibin97/go-ognl"
)

type ColumnValueExtractorParentResultStructSelector struct {
	selector string
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorParentResultStructSelector{}

func (x *ColumnValueExtractorParentResultStructSelector) Name() string {
	return "parent-result-struct-selector-column-value-extractor"
}

func (x *ColumnValueExtractorParentResultStructSelector) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	if reflect_util.IsNil(task.ParentRawResult) {
		return nil, nil
	}
	return ognl.Get(task.ParentRawResult, x.selector).Value(), nil
}

func (x *ColumnValueExtractorParentResultStructSelector) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorParentResultStructSelector) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

// ParentResultStructSelector Extract the value based on the struct selector expression passed in from parent raw result
func ParentResultStructSelector(structSelector string) *ColumnValueExtractorParentResultStructSelector {
	return &ColumnValueExtractorParentResultStructSelector{
		selector: structSelector,
	}
}
