package schema

// ColumnValueConvertor This ColumnValueConvertor is used to convert the extracted column value to the corresponding storage medium value
//
type ColumnValueConvertor interface {

	// Convert The method actually responsible for the type conversion
	//
	// table: The table corresponding to the value to be converted
	// column: The column corresponding to the value to be converted
	// columnValue: The value to convert
	//
	// return:
	//    any: The value of the transformed column
	//    *schema.Diagnostics: Any message you want the user to see. These are usually error reports or warnings
	Convert(table *Table, column *Column, columnValue any) (any, *Diagnostics)
}
