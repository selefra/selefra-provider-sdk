package column_value_extractor

import (
	"context"
	"errors"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/md5_util"
	"strings"
)

type ColumnValueExtractorPrimaryKeysID struct {
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorPrimaryKeysID{}

func (x *ColumnValueExtractorPrimaryKeysID) Name() string {
	return "primary-keys-id-column-value-extractor"
}

func (x *ColumnValueExtractorPrimaryKeysID) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {

	table := task.Table
	diagnostics := schema.NewDiagnostics()

	if len(table.GetPrimaryKeys()) == 0 {
		return nil, diagnostics.AddErrorMsg(BuildExtractErrMsg(x, table, column, "table not have primary keys"))
	}

	columnValues := make([]string, 0)
	for _, columnName := range table.GetPrimaryKeys() {
		value, err := row.GetString(columnName)
		if err != nil {
			diagnostics.AddErrorMsg(BuildExtractErr(x, table, column, err))
		}
		columnValues = append(columnValues, value)
	}

	value, err := md5_util.Md5String(strings.Join(columnValues, " | "))
	if err != nil {
		return nil, diagnostics.AddErrorMsg(BuildExtractErr(x, table, column, err))
	}

	return value, diagnostics
}

func (x *ColumnValueExtractorPrimaryKeysID) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return table.GetPrimaryKeys()
}

func (x *ColumnValueExtractorPrimaryKeysID) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if len(table.GetPrimaryKeys()) == 0 {
		diagnostics.AddErrorMsg(BuildValidateErr(x, table, column, errors.New("table not have primary keys")))
	}

	return diagnostics
}

func PrimaryKeysID() *ColumnValueExtractorPrimaryKeysID {
	return &ColumnValueExtractorPrimaryKeysID{}
}
