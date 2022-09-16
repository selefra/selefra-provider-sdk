package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/md5_util"
	"strings"
)

type ColumnValueExtractorParentPrimaryKeysID struct {
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorParentPrimaryKeysID{}

func (x *ColumnValueExtractorParentPrimaryKeysID) Name() string {
	return "parent-primary-keys-id-column-value-extractor"
}

func (x *ColumnValueExtractorParentPrimaryKeysID) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	if task.ParentTable == nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErrMsg(x, task.Table, column, "parent table is nil"))
	}

	if task.ParentRow == nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErrMsg(x, task.Table, column, "parent row is nil"))
	}

	// The primary key must be set when using this method, otherwise an error message will be thrown
	if len(task.ParentTable.GetPrimaryKeys()) == 0 {
		return nil, diagnostics.AddErrorMsg(BuildExtractErrMsg(x, task.Table, column, "parent table not have primary key"))
	}

	columnValues := make([]string, 0)
	for _, parentColumnName := range task.ParentTable.GetPrimaryKeys() {
		value, err := task.ParentRow.GetString(parentColumnName)
		if err != nil {
			diagnostics.AddErrorMsg(BuildExtractErr(x, task.Table, column, err))
		} else {
			columnValues = append(columnValues, value)
		}
	}
	if diagnostics.HasError() {
		return nil, diagnostics
	}

	value, err := md5_util.Md5String(strings.Join(columnValues, " | "))
	if err != nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErr(x, task.Table, column, err))
	}

	return value, diagnostics
}

func (x *ColumnValueExtractorParentPrimaryKeysID) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	// This is the column of the dependency itself. If it is the parent of the dependency, it does not count. You do not have to declare it here
	return nil
}

func (x *ColumnValueExtractorParentPrimaryKeysID) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if parentTable == nil {
		return diagnostics.AddErrorMsg(BuildExtractErrMsg(x, table, column, "parent table is nil"))
	}

	// The primary key must be set when using this method, otherwise an error message will be thrown
	if len(parentTable.GetPrimaryKeys()) == 0 {
		return diagnostics.AddErrorMsg(BuildExtractErrMsg(x, table, column, "parent table not have primary key"))
	}

	return nil
}

func ParentPrimaryKeysID() *ColumnValueExtractorParentPrimaryKeysID {
	return &ColumnValueExtractorParentPrimaryKeysID{}
}
