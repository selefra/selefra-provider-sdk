package column_value_extractor

import (
	"context"
	"errors"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/md5_util"
	"strings"
)

// ColumnValueExtractorColumnsValueMd5 Generates a value from the MD5 of the values of the other columns in the current row
type ColumnValueExtractorColumnsValueMd5 struct {

	// The columns to MD5 are in order
	columnNameSlice []string
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorColumnsValueMd5{}

func (x *ColumnValueExtractorColumnsValueMd5) Name() string {
	return "columns-value-md5-column-value-extractor"
}

func (x *ColumnValueExtractorColumnsValueMd5) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	columnValues := make([]string, 0)
	for _, columnName := range x.columnNameSlice {
		value, err := row.GetString(columnName)
		if err != nil {
			return nil, diagnostics.AddErrorMsg(BuildExtractErr(x, task.Table, column, err))
		}
		columnValues = append(columnValues, value)
	}

	md5String, err := md5_util.Md5String(strings.Join(columnValues, " | "))
	if err != nil {
		return nil, schema.NewDiagnostics().AddErrorMsg(BuildExtractErr(x, task.Table, column, err))
	}

	return md5String, nil
}

func (x *ColumnValueExtractorColumnsValueMd5) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return x.columnNameSlice
}

func (x *ColumnValueExtractorColumnsValueMd5) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()

	if len(x.columnNameSlice) == 0 {
		diagnostics.AddErrorMsg(BuildValidateErr(x, table, column, errors.New("at least one column must be specified")))
	}

	for _, columnName := range x.columnNameSlice {
		if !table.Runtime().ContainsColumnName(columnName) {
			diagnostics.AddErrorMsg(BuildValidateErr(x, table, column, errors.New(fmt.Sprintf("column %s not found", columnName))))
		}
	}

	return diagnostics
}

func ColumnsValueMd5(columnNameSlice ...string) *ColumnValueExtractorColumnsValueMd5 {
	return &ColumnValueExtractorColumnsValueMd5{
		columnNameSlice: columnNameSlice,
	}
}
