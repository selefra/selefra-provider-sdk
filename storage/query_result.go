package storage

import "github.com/selefra/selefra-provider-sdk/provider/schema"

// QueryResult Represents the result of a query
type QueryResult interface {

	// ------------------------------------------------- row by row ----------------------------------------------------

	// Next Attempts to switch to the next result and returns whether the switch was successful
	Next() bool

	// Decode the current ROW as an item, which should be the address of a struct
	Decode(item any) *schema.Diagnostics

	// Values Returns the value of each column of the current ROW for use when there is no structure
	Values() ([]any, *schema.Diagnostics)

	// ValuesMap Returns the value of each column in the current ROW, with the column name as key and the column value as value
	ValuesMap() (map[string]any, *schema.Diagnostics)

	// ------------------------------------------------- A shuttle ---------------------------------------------------------

	// ReadRows Returns the result set as a RowSet
	// @params rowLimit: Read the specified number of rows. If the number of rows is limited to a negative number, all reads at once are unrestricted
	ReadRows(rowLimit int) (*schema.Rows, *schema.Diagnostics)

	// ------------------------------------------------- Other auxiliary methods ---------------------------------------------------

	// GetColumnNames Gets the names of all the columns queried
	GetColumnNames() []string

	// Closeable The query result can be disabled. Most storage media need to shut down resources after reading the query result
	Closeable

	// GetRawQueryResult Original query results are allowed, but are not recommended
	GetRawQueryResult() any
}
