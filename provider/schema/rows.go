package schema

import (
	"encoding/json"
	"errors"
	"github.com/spf13/cast"
)

var (
	ErrorColumnNotEnough = errors.New("column not enough!")

	ErrorRowNotEnough = errors.New("row not enough!")

	ErrorColumnAlreadyExists = errors.New("Column already exists")

	ErrorColumnNotExists = errors.New("column not exists")
)

// Rows Represents a number of rows of data in a table, a collection of rows
type Rows struct {

	// The name of the column of the matrix
	columnNames []string

	// The value of the column and column matrix
	matrix [][]any

	// Column and column indexes when you write to a matrix, only sequential writes are allowed, so the column labels don't need to be saved
	writeRowIndex int
}

// ------------------------------------------------- Constructor -------------------------------------------------------

func NewRows(columnNames ...string) *Rows {
	rows := &Rows{
		columnNames: make([]string, 0),
		matrix:      make([][]any, 0),
	}

	// Class if the column name is set
	for _, columnName := range columnNames {
		rows.columnNames = append(rows.columnNames, columnName)
	}

	return rows
}

// ------------------------------------------------- The column name ---------------------------------------------------

// AddColumnName Add a column to the table
func (x *Rows) AddColumnName(columnName string) error {
	// When the matrix is assigned and the number of rows is greater than 1, it is not allowed to change the number of columns
	// Columns are allowed when only one row has been written
	if x.RowCount() > 1 {
		return errors.New("not allow change column after matrix have great than two lines")
	}
	x.columnNames = append(x.columnNames, columnName)
	return nil
}

// AddColumnNames Add multiple columns at a time
func (x *Rows) AddColumnNames(columnName []string) error {
	for _, columnName := range columnName {
		if err := x.AddColumnName(columnName); err != nil {
			return err
		}
	}
	return nil
}

// SetColumnNames Set the column name. The matrix value is reset when the column name is set
func (x *Rows) SetColumnNames(columnNames []string) *Rows {
	x.columnNames = columnNames
	x.matrix = make([][]any, 0)
	return x
}

// GetColumnNames Gets all column names
func (x *Rows) GetColumnNames() []string {
	return x.columnNames
}

// ------------------------------------------------- Matrix operation --------------------------------------------------

// SetMatrix Sets the value of the matrix
func (x *Rows) SetMatrix(matrix [][]any) error {

	// The column names must be set before setting the matrix
	if len(x.columnNames) == 0 {
		return errors.New("Rows.SetMatrix must call after set columns")
	}

	// Check that the number of columns in the matrix matches the number of column names
	for _, row := range matrix {
		if len(row) != len(x.columnNames) {
			return ErrorRowNotEnough
		}
	}

	// Check passed and save the matrix
	x.matrix = matrix

	return nil
}

// GetMatrix Returns the inner matrix
func (x *Rows) GetMatrix() [][]any {
	return x.matrix
}

// ------------------------------------------------- Line operations ---------------------------------------------------

// AppendRowValues Write a line
func (x *Rows) AppendRowValues(row []any) error {

	// Check that the number of columns is correct
	if len(x.columnNames) != len(row) {
		return errors.New("Wrong number of columns")
	}

	// And then I'll say a line
	x.matrix = append(x.matrix, row)
	// Moves the write pointer down one line
	x.writeRowIndex++

	return nil
}

// AppendRow Write a line
func (x *Rows) AppendRow(row *Row) error {

	// Check that the column structures are the same and refuse to write if they are different
	if len(x.columnNames) != len(row.columnNames) {
		return errors.New("Wrong number of columns")
	}
	for index := range x.columnNames {
		if row.columnNames[index] != x.columnNames[index] {
			return errors.New("Wrong column name")
		}
	}

	// Check that the number of column values is equal
	values := row.GetValues()
	if len(x.columnNames) != len(values) {
		return errors.New("Wrong number of columns")
	}

	// All right, check passed. Ready to write
	return x.AppendRowValues(values)
}

// GetRow Gets the specified Row, converts it to Row and returns
func (x *Rows) GetRow(rowIndex int) (*Row, error) {

	values, err := x.GetRowValues(rowIndex)
	if err != nil {
		return nil, err
	}

	return NewRow(x.GetColumnNames()...).SetValuesIgnoreError(values), nil
}

// GetRowValues Gets the values of all columns of the specified row
func (x *Rows) GetRowValues(rowIndex int) ([]any, error) {
	if rowIndex >= len(x.matrix) {
		return nil, ErrorRowNotEnough
	}
	return x.matrix[rowIndex], nil
}

// ------------------------------------------------- Operation of cells ------------------------------------------------

// GetColumnValue Get the value of a given column for a given row. Note that this method is inefficient and should be avoided if possible
func (x *Rows) GetColumnValue(rowIndex int, columnName string) (any, error) {
	if rowIndex >= x.RowCount() {
		return nil, errors.New("RowIndex out of range")
	}
	columnIndex := -1
	for index, matrixColumnName := range x.columnNames {
		if matrixColumnName == columnName {
			columnIndex = index
		}
	}
	if columnIndex == -1 {
		return nil, errors.New("column not found")
	}
	// It could be a blank line
	if columnIndex >= len(x.matrix[rowIndex]) {
		return nil, nil
	}
	return x.matrix[rowIndex][columnIndex], nil
}

func (x *Rows) GetCellValue(rowIndex, columnIndex int) (any, error) {
	if rowIndex >= x.RowCount() {
		return nil, errors.New("RowIndex out of range")
	}
	if columnIndex == -1 {
		return nil, errors.New("column not found")
	}
	// It could be a blank line
	if columnIndex >= len(x.matrix[rowIndex]) {
		return nil, nil
	}
	return x.matrix[rowIndex][columnIndex], nil
}

func (x *Rows) GetCellValueOrDefault(rowIndex, columnIndex int, defaultValue any) any {
	value, err := x.GetCellValue(rowIndex, columnIndex)
	if err != nil {
		return defaultValue
	}
	return value
}

func (x *Rows) GetCellIntValueOrDefault(rowIndex, columnIndex int, defaultValue int) int {
	rawValue, err := x.GetCellValue(rowIndex, columnIndex)
	if err != nil {
		return defaultValue
	}
	value, err := cast.ToIntE(rawValue)
	if err != nil {
		return defaultValue
	}
	return value
}

func (x *Rows) GetCellStringValueOrDefault(rowIndex, columnIndex int, defaultValue string) string {
	rawValue, err := x.GetCellValue(rowIndex, columnIndex)
	if err != nil {
		return defaultValue
	}
	value, err := cast.ToStringE(rawValue)
	if err != nil {
		return defaultValue
	}
	return value
}

// I'm going to write a cell in the table, and I'm only allowed to write cells sequentially
func (x *Rows) Write(cellValue any) {

	// If you don't have enough rows, you add a new row
	for x.RowCount() <= x.writeRowIndex {
		x.matrix = append(x.matrix, []any{})
	}

	// Then append to the corresponding row
	x.matrix[x.writeRowIndex] = append(x.matrix[x.writeRowIndex], cellValue)
}

// WriteNewLine Write a newline
func (x *Rows) WriteNewLine() error {

	// Check whether the number of rows is valid
	if x.writeRowIndex != x.RowCount()-1 {
		return ErrorRowNotEnough
	}

	// Check that the number of columns is valid. Empty rows are allowed, but rows that are not aligned with column names are not allowed
	lastRowColumnCount := len(x.matrix[x.writeRowIndex])
	if lastRowColumnCount != 0 && lastRowColumnCount != len(x.columnNames) {
		return ErrorColumnNotEnough
	}

	// Number of lines plus one
	x.writeRowIndex++

	return nil
}

// ------------------------------------------------- Matrix transformation ---------------------------------------------

// SplitRowByRow Partition the matrix into matrices with only one row
func (x *Rows) SplitRowByRow() []*Row {
	result := make([]*Row, 0)
	for _, row := range x.matrix {
		r := NewRow(x.GetColumnNames()...)
		err := r.SetValues(row)
		if err != nil {
			// ignored
		} else {
			result = append(result, r)
		}
	}
	return result
}

// ToRow Convert Rows to Row only if there are 0 or 1 Rows
func (x *Rows) ToRow() (*Row, error) {

	row := NewRow(x.GetColumnNames()...)

	switch x.RowCount() {
	case 0:
		// Don't set the value
	case 1:
		// Read the value set on the first row
		values, err := x.GetRowValues(0)
		if err != nil {
			return nil, err
		}
		err = row.SetValues(values)
		if err != nil {
			return nil, err
		}
	default:
	}

	return row, nil
}

// ------------------------------------------------- count -------------------------------------------------------------

// RowCount How many lines
func (x *Rows) RowCount() int {
	return len(x.matrix)
}

func (x *Rows) IsEmpty() bool {
	return x.RowCount() == 0
}

// ColumnCount How many columns
func (x *Rows) ColumnCount() int {
	return len(x.columnNames)
}

// ------------------------------------------------- The first line operation ------------------------------------------

// The first row operation is for when there is only one row in Rows

func (x *Rows) GetFirstRowColumnValue(columnName string) (any, error) {
	return x.GetColumnValue(0, columnName)
}

func (x *Rows) GetFirstRowColumnValueAsStringOrDefault(columnName string, defaultValue string) string {
	value, err := x.GetFirstRowColumnValue(columnName)
	if err != nil {
		return defaultValue
	}
	e, err := cast.ToStringE(value)
	if err != nil {
		return defaultValue
	}
	return e
}

func (x *Rows) GetFirstRowColumnValueAsIntOrDefault(columnName string, defaultValue int) int {
	value, err := x.GetFirstRowColumnValue(columnName)
	if err != nil {
		return defaultValue
	}
	e, err := cast.ToIntE(value)
	if err != nil {
		return defaultValue
	}
	return e
}

func (x *Rows) GetFirstRowColumnValueAsBoolOrDefault(columnName string, defaultValue bool) bool {
	value, err := x.GetFirstRowColumnValue(columnName)
	if err != nil {
		return defaultValue
	}
	e, err := cast.ToBoolE(value)
	if err != nil {
		return defaultValue
	}
	return e
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *Rows) String() string {
	lineSlice := make([]map[string]any, 0)
	for _, row := range x.matrix {
		lineMap := make(map[string]any, 0)
		for index, column := range row {
			lineMap[x.columnNames[index]] = column
		}
		lineSlice = append(lineSlice, lineMap)
	}
	marshal, err := json.Marshal(lineSlice)
	if err != nil {
		return "error: " + err.Error()
	}
	return string(marshal)
}

// --------------------------------------------------------------------------------------------------------------------
