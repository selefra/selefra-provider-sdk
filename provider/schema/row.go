package schema

import (
	"encoding/json"
	"github.com/spf13/cast"
)

// Row Represents a row in a matrix or database
type Row struct {

	// The column names are stored in the array in their original order
	columnNames []string

	//
	columnValueMap map[string]any
}

func NewRow(columnNameSlice ...string) *Row {

	// Each column is initialized with a value of nil, which is initialized by using the key of the Map as a whitelist
	columnValueMap := make(map[string]any)
	for _, columnName := range columnNameSlice {
		columnValueMap[columnName] = nil
	}

	return &Row{
		columnNames:    columnNameSlice,
		columnValueMap: columnValueMap,
	}

}

// ------------------------------------------------- The column value operation ----------------------------------------

// Get Gets the column value based on the column name
func (x *Row) Get(columnName string) (any, error) {
	value, exists := x.columnValueMap[columnName]
	if !exists {
		return nil, ErrorColumnNotExists
	}
	return value, nil
}

func (x *Row) GetString(columnName string) (string, error) {
	value, err := x.Get(columnName)
	if err != nil {
		return "", err
	}
	stringValue, err := cast.ToStringE(value)
	if err != nil {
		return "", err
	}
	return stringValue, nil
}

// GetStringOrDefault The fetch column value is returned as String, or the default value in case of an error
func (x *Row) GetStringOrDefault(columnName, defaultValue string) string {
	value, err := x.GetString(columnName)
	if err != nil {
		return defaultValue
	}
	return value
}

func (x *Row) GetInt(columnName string) (int, error) {
	value, err := x.Get(columnName)
	if err != nil {
		return 0, err
	}
	intValue, err := cast.ToIntE(value)
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

func (x *Row) GetIntOrDefault(columnName string, defaultValue int) int {
	value, err := x.GetInt(columnName)
	if err != nil {
		return defaultValue
	}
	return value
}

// Set Set the column value based on the column name
func (x *Row) Set(columnName string, value any) (any, error) {
	oldValue, exists := x.columnValueMap[columnName]
	if !exists {
		return nil, ErrorColumnNotExists
	}
	x.columnValueMap[columnName] = value
	return oldValue, nil
}

// GetValues Returns all values of the current row in order
func (x *Row) GetValues() []any {
	values := make([]any, 0)
	// Traversal by column name, you need to preserve column order
	for _, columnName := range x.columnNames {
		values = append(values, x.columnValueMap[columnName])
	}
	return values
}

// SetValues Sets the values for all columns of the row at once
func (x *Row) SetValues(values []any) error {
	if len(values) != len(x.columnNames) {
		return ErrorColumnNotEnough
	}
	// Set in order
	for index, columnName := range x.columnNames {
		x.columnValueMap[columnName] = values[index]
	}
	return nil
}

// SetValuesIgnoreError Set values for all columns of the row at once, ignoring errors
func (x *Row) SetValuesIgnoreError(values []any) *Row {
	_ = x.SetValues(values)
	return x
}

// ------------------------------------------------- The column name operation -----------------------------------------

// AddColumnName Add a column
func (x *Row) AddColumnName(columnName string) error {

	// Duplicate column names are not allowed
	if _, exists := x.columnValueMap[columnName]; exists {
		return ErrorColumnAlreadyExists
	}

	// Operating together
	x.columnNames = append(x.columnNames, columnName)
	x.columnValueMap[columnName] = nil

	return nil
}

// AddColumnNames Add multiple columns at a time
func (x *Row) AddColumnNames(columnNames []string) error {
	for _, columnName := range columnNames {
		if err := x.AddColumnName(columnName); err != nil {
			return err
		}
	}
	return nil
}

// SetColumnNames Setting the current column is equivalent to resetting, which is an override setting
func (x *Row) SetColumnNames(columnNames []string) error {

	// Clean and reset all column values
	columnValueMap := make(map[string]any)
	for _, columnName := range columnNames {
		columnValueMap[columnName] = nil
	}

	// You're essentially reinitializing
	x.columnNames = columnNames
	x.columnValueMap = columnValueMap

	return nil
}

// GetColumnNames Gets all current column's name return as string slice
func (x *Row) GetColumnNames() []string {
	return x.columnNames
}

// ------------------------------------------------- The numerical statistical -----------------------------------------

// ColumnCount How many columns count
func (x *Row) ColumnCount() int {
	return len(x.columnNames)
}

// ------------------------------------------------- Row matrix transformation -----------------------------------------

// ToRows Converts the current row to Rows, if occur error then ignore it
func (x *Row) ToRows() *Rows {
	rows := NewRows(x.GetColumnNames()...)
	_ = rows.AppendRowValues(x.GetValues())
	return rows
}

// ToRowsE Converts the current row to Rows, if occur error then return
func (x *Row) ToRowsE() (*Rows, error) {
	rows := NewRows(x.GetColumnNames()...)
	err := rows.AppendRowValues(x.GetValues())
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *Row) String() string {
	marshal, err := json.Marshal(x.columnValueMap)
	if err != nil {
		return "error: " + err.Error()
	}
	return string(marshal)
}

// ------------------------------------------------- -------------------------------------------------------------------
