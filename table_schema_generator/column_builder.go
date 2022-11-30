package table_schema_generator

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/pointer"
)

type ColumnBuilder struct {
	column *schema.Column
}

func NewColumnBuilder() *ColumnBuilder {
	return &ColumnBuilder{
		column: &schema.Column{},
	}
}

func (x *ColumnBuilder) ColumnName(columnName string) *ColumnBuilder {
	x.column.ColumnName = columnName
	return x
}

func (x *ColumnBuilder) ColumnType(columnType schema.ColumnType) *ColumnBuilder {
	x.column.Type = columnType
	return x
}

func (x *ColumnBuilder) Description(description string) *ColumnBuilder {
	x.column.Description = description
	return x
}

func (x *ColumnBuilder) Extractor(extractor schema.ColumnValueExtractor) *ColumnBuilder {
	if extractor != nil {
		x.column.Extractor = extractor
	}
	return x
}

func (x *ColumnBuilder) Options(options *schema.ColumnOptions) *ColumnBuilder {
	if options != nil {
		x.column.Options = *options
	}
	return x
}

func (x *ColumnBuilder) SetUnique() *ColumnBuilder {
	x.column.Options.Unique = pointer.TruePointer()
	return x
}

func (x *ColumnBuilder) SetNotNull() *ColumnBuilder {
	x.column.Options.NotNull = pointer.TruePointer()
	return x
}

func (x *ColumnBuilder) Build() *schema.Column {
	return x.column
}
