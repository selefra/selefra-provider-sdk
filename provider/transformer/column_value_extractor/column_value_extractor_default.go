package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/songzhibin97/go-ognl"
	"strings"
)

var DefaultColumnValueExtractor schema.ColumnValueExtractor = Default()

type ColumnValueExtractorDefault struct {
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorDefault{}

func (x *ColumnValueExtractorDefault) Name() string {
	return "default-column-value-extractor"
}

func (x *ColumnValueExtractorDefault) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any,  task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	columnName := column.ColumnName
	if columnName == "" {
		return nil, nil
	}

	value := ognl.Get(result, columnName).Value()
	if value != nil {
		return value, nil
	}

	columnName = UnderscoreToUpperCamelCase(columnName)
	return ognl.Get(result, columnName).Value(), nil
}

func (x *ColumnValueExtractorDefault) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorDefault) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

func Default() *ColumnValueExtractorDefault {
	return &ColumnValueExtractorDefault{}
}

func UnderscoreToUpperCamelCase(s string) string {
	s = strings.Replace(s, "_", " ", -1)
	s = strings.Title(s)
	return strings.Replace(s, " ", "", -1)
}
