package column_value_extractor

import (
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// BuildExtractErr build error message for extract
func BuildExtractErr(extractor schema.ColumnValueExtractor, table *schema.Table, column *schema.Column, err error) string {
	return BuildExtractErrMsg(extractor, table, column, err.Error())
}

func BuildExtractErrMsg(extractor schema.ColumnValueExtractor, table *schema.Table, column *schema.Column, msg string, args ...any) string {
	return fmt.Sprintf("table %s column %s extractor %s extract error: %s", table.TableName, column.ColumnName, extractor.Name(), fmt.Sprintf(msg, args...))
}

// BuildValidateErr build error message for validate
func BuildValidateErr(extractor schema.ColumnValueExtractor, table *schema.Table, column *schema.Column, err error) string {
	return BuildValidateErrMsg(extractor, table, column, err.Error())
}

func BuildValidateErrMsg(extractor schema.ColumnValueExtractor, table *schema.Table, column *schema.Column, msg string, args ...any) string {
	return fmt.Sprintf("table %s column %s extractor %s validate error: %s", table.TableName, column.ColumnName, extractor.Name(), fmt.Sprintf(msg, args...))
}
