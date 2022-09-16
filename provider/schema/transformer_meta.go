package schema

// TransformerMeta data transformation meta information
type TransformerMeta struct {

	// if you api will return some valid string, you can config on this field
	//
	// For example, if the value of an int field is not available, the API will return a uniform invalid value, such as N/A.
	// Even though it is an int field, the API will return a string it's value is "N/A", so you can use this configuration
	// to be compatible with this situation. The underlying converter will check and ignore any invalid value configured.
	//
	// Note that the checker will only check for fields that are not String. If an invalid value is specified on a String
	// field, it will be stored because the checker cannot determine whether it is a valid value or an invalid value, so
	// it plays it safe
	DefaultColumnValueConvertorBlackList []string

	// If you do not want to use the default type converter, you can use your own custom type converter.
	// In order to realize your own ColumnValueConvertor, implementation schema.ColumnValueConvertor interface
	//
	// If you do not configure this field, a default type converter will be initialized, default
	// ColumnValueConvertor is column_value_convertor.DefaultColumnValueConvertor
	ColumnValueConvertor ColumnValueConvertor
}

func (x *TransformerMeta) IsUseDefaultColumnValueConvertor() bool {
	return len(x.DefaultColumnValueConvertorBlackList) != 0
}
