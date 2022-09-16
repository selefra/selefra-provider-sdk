package schema

import (
	"context"
)

// Used to define how column values are parsed, this is just interface definition, will some default implementation be done in Transformer

// ColumnValueExtractor To indicate how to extract the value of this column from the result returned by the API's interface
// See package transformer.value_extractor for the default implementation
type ColumnValueExtractor interface {

	// Name The name of this ColumnValueExtractor
	Name() string

	// Extract The method actually responsible for extracting the value
	// ctx:
	// clientMeta:
	// task:
	// row:
	// column:
	// result:
	// return any
	// return *Diagnostics
	Extract(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, row *Row, column *Column, result any) (any, *Diagnostics)

	// DependencyColumnNames If the values of this class depend on other columns,
	// the data in the same row will also be parsed according to the DAG dependency topology between the columns
	DependencyColumnNames(ctx context.Context, clientMeta *ClientMeta, parentTable *Table, table *Table, column *Column) []string

	// Validate This method is called to check when the runtime is initialized to detect errors as early as possible
	Validate(ctx context.Context, clientMeta *ClientMeta, parentTable *Table, table *Table, column *Column) *Diagnostics
}
