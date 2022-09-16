package schema

// Column one column definition in table
type Column struct {

	// Column's name
	ColumnName string

	// Column's type, see schema.ColumnType, Columns must specify a type
	Type ColumnType

	// Column comments will be added to the table when the table is created
	Description string

	// To indicate how to extract the value of this column from the response content of the API
	Extractor ColumnValueExtractor

	// Some options for creating columns, such as uniq, not null
	Options ColumnOptions

	// Column's runtime
	runtime ColumnRuntime
}

func (x *Column) Runtime() *ColumnRuntime {
	return &x.runtime
}
