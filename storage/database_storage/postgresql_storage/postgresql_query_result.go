package postgresql_storage

import (
	"github.com/jackc/pgx/v4"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
)

type PostgresqlQueryResult struct {
	rows pgx.Rows
}

var _ storage.QueryResult = &PostgresqlQueryResult{}

func (x *PostgresqlQueryResult) Next() bool {
	return x.rows.Next()
}

func (x *PostgresqlQueryResult) Decode(item any) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	err := x.rows.Scan(item)
	if err != nil {
		diagnostics.AddErrorMsg("PostgresqlQueryResult decode error: %s", err.Error())
	}
	return diagnostics
}

func (x *PostgresqlQueryResult) Values() ([]any, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	values, err := x.rows.Values()
	if err != nil {
		diagnostics.AddErrorMsg("PostgresqlQueryResult values error: %s", err.Error())
	}
	return values, diagnostics
}

func (x *PostgresqlQueryResult) ValuesMap() (map[string]any, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	valuesMap := make(map[string]any, 0)
	values, d := x.Values()
	if diagnostics.AddDiagnostics(d).HasError() {
		return valuesMap, diagnostics
	}
	columnNames := x.GetColumnNames()
	if len(columnNames) != len(values) {
		return nil, diagnostics.AddErrorMsg("PostgresqlQueryResult valuesMap error: column length mismatch")
	}
	for index, columnName := range columnNames {
		valuesMap[columnName] = values[index]
	}
	return valuesMap, nil
}

func (x *PostgresqlQueryResult) ReadRows(rowLimit int) (*schema.Rows, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	rows := schema.NewRows().SetColumnNames(x.GetColumnNames())
	for (rowLimit < 0 || rows.RowCount() < rowLimit) && x.rows.Next() {
		values, err := x.rows.Values()
		if err != nil {
			return rows, diagnostics.AddErrorMsg("PostgresqlQueryResult read rows error: %s", err.Error())
		}
		err = rows.AppendRowValues(values)
		if err != nil {
			return nil, diagnostics.AddErrorMsg("PostgresqlQueryResult read rows error: %s", err.Error())
		}
	}
	return rows, nil
}

func (x *PostgresqlQueryResult) GetColumnNames() []string {
	columnsNames := make([]string, 0)
	for _, column := range x.rows.FieldDescriptions() {
		columnsNames = append(columnsNames, string(column.Name))
	}
	return columnsNames
}

func (x *PostgresqlQueryResult) Close() *schema.Diagnostics {
	x.rows.Close()
	return nil
}

func (x *PostgresqlQueryResult) GetRawQueryResult() any {
	return x.rows
}
