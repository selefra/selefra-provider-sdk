package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

type ColumnValueExtractorParentColumnValue struct {
	parentTableColumnName string
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorParentColumnValue{}

func (x *ColumnValueExtractorParentColumnValue) Name() string {
	return "parent-column-value-column-value-extractor"
}

func (x *ColumnValueExtractorParentColumnValue) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	if task.ParentRow == nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErrMsg(x, task.Table, column, "parent row is nil"))
	}

	value, err := task.ParentRow.Get(x.parentTableColumnName)
	if err != nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErr(x, task.Table, column, err))
	}

	return value, diagnostics
}

func (x *ColumnValueExtractorParentColumnValue) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	// This is the column of the dependency itself. If it is the parent of the dependency, it does not count. You do not have to declare it here
	return nil
}

func (x *ColumnValueExtractorParentColumnValue) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if parentTable == nil {
		return diagnostics.AddErrorMsg(BuildExtractErrMsg(x, table, column, "parent table is nil"))
	}

	if !parentTable.Runtime().ContainsColumnName(x.parentTableColumnName) {
		return diagnostics.AddErrorMsg(BuildExtractErrMsg(x, table, column, "parent table not have column %s", x.parentTableColumnName))
	}

	return nil
}

func (x *ColumnValueExtractorParentColumnValue) GetParentTableColumnName() string {
	return x.parentTableColumnName
}

func ParentColumnValue(parentTableColumnName string) *ColumnValueExtractorParentColumnValue {
	return &ColumnValueExtractorParentColumnValue{
		parentTableColumnName: parentTableColumnName,
	}
}
