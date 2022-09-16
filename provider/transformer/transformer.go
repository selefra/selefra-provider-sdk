package transformer

import (
	"context"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-utils/pkg/runtime_util"
	"github.com/selefra/selefra-utils/pkg/string_util"
	"strings"
)

// Transformer This is mainly to encapsulate some of the data format conversion logic
type Transformer struct {

	// The extractor may need ClientMeta, which is held for passing
	clientMeta *schema.ClientMeta

	// The type converter is used to convert the value to the value corresponding to the Storage
	typeConvertor schema.ColumnValueConvertor

	// The Provider developer has control over how errors are handled during the conversion process
	errorHandlerMeta *schema.ErrorsHandlerMeta
}

func NewTransformer(clientMeta *schema.ClientMeta, typeConvertor schema.ColumnValueConvertor, errorHandlerMeta *schema.ErrorsHandlerMeta) *Transformer {
	return &Transformer{
		clientMeta:       clientMeta,
		typeConvertor:    typeConvertor,
		errorHandlerMeta: errorHandlerMeta,
	}
}

// TransformResult Convert the results obtained from the DataSource to the data in the table. Convert only one result ata time. Batch conversion is not supported
func (x *Transformer) TransformResult(ctx context.Context, client any, task *schema.DataSourcePullTask, result any) (*schema.Row, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	row := schema.NewRow()
	table := task.Table
	isNeedIgnoreCellTransformerError := x.errorHandlerMeta.IsIgnore(schema.IgnoredErrorOnTransformerCell)

	// Columns cannot be empty
	if len(table.Runtime().ColumnExtractorSorted) == 0 {
		return nil, diagnostics.AddErrorMsg(x.buildErrorMsg(table, nil, "Table.Runtime().ColumnExtractorSorted is empty"))
	}

	// You have to have the data to do the conversion
	if result == nil {
		return nil, diagnostics.AddErrorMsg(x.buildErrorMsg(table, nil, "result must not nil"))
	}

	// It is resolved in topological order, because there may be dependencies between columns, and the relationship between columns may be a DAG
	for _, column := range table.Runtime().ColumnExtractorSorted {

		// column's name
		err := row.AddColumnName(column.ColumnName)
		if err != nil {
			diagnostics.AddErrorMsg(x.buildErrorMsg(table, column, err.Error()))
			continue
		}

		// Extract the value of the column
		value, d := x.extractColumn(ctx, client, task, column, row, result)
		hasError := d != nil && d.HasError()

		if hasError {

			// log runtime context if occurs error help resolve problem
			msg := string_util.NewStringBuilder()
			msg.WriteString(fmt.Sprintf("taskId = %s, table %s column %s transformer error: %s", task.TaskId, table.TableName, column.ColumnName, d.ToString()))
			msg.WriteString("\n row result raw value: ").WriteString(fmt.Sprintf("%s", result))
			msg.WriteString("\nStack: \n")
			msg.WriteString(runtime_util.Stack())
			x.clientMeta.Error(msg.String())

			// if has error, ensure value be nil
			value = nil

		}

		// need ignore error, just write empty value
		if hasError && isNeedIgnoreCellTransformerError {
			// do nothing
		} else {
			// or else, record transformer return information
			diagnostics.AddDiagnostics(d)
		}

		// column's value, may be empty, just write it
		_, err = row.Set(column.ColumnName, value)
		if err != nil {
			diagnostics.AddErrorMsg(x.buildErrorMsg(table, column, err.Error()))
			continue
		}
	}

	return row, diagnostics
}

func (x *Transformer) buildErrorMsg(table *schema.Table, column *schema.Column, msg string, args ...string) string {
	msgBuilder := strings.Builder{}

	if table != nil {
		msgBuilder.WriteString("table ")
		msgBuilder.WriteString(table.TableName)
		msgBuilder.WriteString(" ")
	}

	if column != nil {
		msgBuilder.WriteString("column ")
		msgBuilder.WriteString(column.ColumnName)
		msgBuilder.WriteString(" ")
	}

	msgBuilder.WriteString("transformer error: ")
	msgBuilder.WriteString(fmt.Sprintf(msg, args))

	return msgBuilder.String()
}

// extract the value of a single column
func (x *Transformer) extractColumn(ctx context.Context, client any, task *schema.DataSourcePullTask, column *schema.Column, row *schema.Row, result any) (extractResult any, diagnostics *schema.Diagnostics) {

	diagnostics = schema.NewDiagnostics()

	// decision which ColumnValueExtractor to use
	extractor := column.Extractor
	if extractor == nil {
		// use the default extractor if columns not config extractor
		extractor = column_value_extractor.DefaultColumnValueExtractor
	}

	// catch panic
	defer func() {
		if err := recover(); err != nil {

			msg := strings.Builder{}
			msg.WriteString(fmt.Sprintf("table %s column %s extractor %s extract error: %s, unable to extract %#v of type %T to %s", task.Table.TableName, column.ColumnName, extractor.Name(), err, result, result, column.Type.String()))
			// if occur error, change result and put error message into diagnostics
			diagnostics.AddErrorMsg(msg.String())
			extractResult = nil

			msg.WriteString("\nStack: \n")
			msg.WriteString(runtime_util.Stack())
			x.clientMeta.Error(msg.String())
		}
	}()

	// extract column value from result
	value, d := extractor.Extract(ctx, x.clientMeta, client, task, row, column, result)
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}

	// attempt to cast type to match storage
	value, d = x.typeConvertor.Convert(task.Table, column, value)
	if diagnostics.AddDiagnostics(d).HasError() {
		// drop raw value if occur error
		return nil, diagnostics
	}

	return value, diagnostics
}

// TODO Add pipeline converter
