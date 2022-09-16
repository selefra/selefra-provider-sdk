package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// A value extractor with a layer around it, so if you don't want to write structs you can do this anonymously in a method way

type ColumnValueExtractorWrapper struct {

	// The following sections are from the ColumnValueExtractor
	name                  string
	extract               func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics)
	dependencyColumnNames func(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string
	validate              func(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorWrapper{}

func (x *ColumnValueExtractorWrapper) Name() string {
	return x.name
}

func (x *ColumnValueExtractorWrapper) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	if x.extract == nil {
		return nil, nil
	}
	return x.extract(ctx, clientMeta, client, task, row, column, result)
}

func (x *ColumnValueExtractorWrapper) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	if x.dependencyColumnNames == nil {
		return nil
	}
	return x.dependencyColumnNames(ctx, clientMeta, parentTable, table, column)
}

func (x *ColumnValueExtractorWrapper) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	if x.validate == nil {
		return schema.NewDiagnostics()
	}
	return x.validate(ctx, clientMeta, parentTable, table, column)
}

func Wrapper(extractorName string,
	extractFunction func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics),
	dependencyColumnNames func(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string,
	validate func(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics) schema.ColumnValueExtractor {
	return &ColumnValueExtractorWrapper{
		name:                  extractorName,
		extract:               extractFunction,
		dependencyColumnNames: dependencyColumnNames,
		validate:              validate,
	}
}

func WrapperExtractFunction(extractFunction func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics)) schema.ColumnValueExtractor {
	return Wrapper("wrapper-extract-function-column-value-extractor", extractFunction, nil, nil)
}
