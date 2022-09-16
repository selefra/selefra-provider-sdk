package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_convertor"
	"github.com/songzhibin97/go-ognl"
)

type ColumnValueExtractorStructSelectorTime struct {
	selector       string
	formatterSlice []column_value_convertor.TimeFormat
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorStructSelectorTime{}

func (x ColumnValueExtractorStructSelectorTime) Name() string {
	return "struct-selector-time-column-value-extractor"
}

func (x ColumnValueExtractorStructSelectorTime) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	value := ognl.Get(result, x.selector).Value()
	if value == nil {
		return nil, nil
	}
	timestamp, err := column_value_convertor.ConvertToTimestamp(value, x.formatterSlice...)
	if err != nil {
		return nil, schema.NewDiagnostics().AddErrorMsg(BuildExtractErrMsg(x, task.Table, column, err.Error()))
	}
	return timestamp, nil
}

func (x ColumnValueExtractorStructSelectorTime) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x ColumnValueExtractorStructSelectorTime) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

func StructSelectorTime(structSelector string, timeFormatter ...string) *ColumnValueExtractorStructSelectorTime {
	formatterSlice := make([]column_value_convertor.TimeFormat, len(timeFormatter))
	for _, formatter := range timeFormatter {
		formatterSlice = append(formatterSlice, column_value_convertor.TimeFormat{
			Formatter:      formatter,
			TimeFormatType: column_value_convertor.TimeFormatTimeOnly,
		})
	}
	return &ColumnValueExtractorStructSelectorTime{
		selector:       structSelector,
		formatterSlice: formatterSlice,
	}
}

func StructSelectorTimeWithTimeFormatType(structSelector string, timeFormatter ...column_value_convertor.TimeFormat) *ColumnValueExtractorStructSelectorTime {
	return &ColumnValueExtractorStructSelectorTime{
		selector:       structSelector,
		formatterSlice: timeFormatter,
	}
}
