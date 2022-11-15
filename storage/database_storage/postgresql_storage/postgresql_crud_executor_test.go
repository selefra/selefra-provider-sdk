package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPostgresqlCRUDExecutor_Query(t *testing.T) {
	diagnostics := schema.NewDiagnostics()

	// test select 1
	sql := "SELECT 1 "
	queryResult, d := testCrudExecutor.Query(context.Background(), sql)
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	rows, d := queryResult.ReadRows(-1)
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	assert.NotNil(t, rows)
	assert.Equal(t, rows.RowCount(), 1)
	assert.Equal(t, rows.ColumnCount(), 1)
	v := rows.GetCellIntValueOrDefault(0, 0, -1)
	assert.Equal(t, v, 1)

	// test params query
	sql = "SELECT * FROM pg_tables WHERE schemaname=$1"
	queryResult, d = testCrudExecutor.Query(context.Background(), sql, "public")
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	rows, d = queryResult.ReadRows(-1)
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	assert.NotNil(t, rows)
}

func TestPostgresqlCRUDExecutor_Exec(t *testing.T) {
	diagnostics := schema.NewDiagnostics()

	// ensure test table exists and empty
	table := getTestTable()
	assert.False(t, diagnostics.Add(testTableAdmin.TableDrop(context.Background(), table)).HasError())
	assert.False(t, diagnostics.Add(testTableAdmin.TableCreate(context.Background(), table)).HasError())

	// test insert data
	sql := "INSERT INTO " + table.TableName + " (id, username, age) VALUES ($1, $2, $3)"
	id := 1
	username := "Tom"
	age := 3
	d := testCrudExecutor.Exec(context.Background(), sql, id, username, age)
	assert.False(t, diagnostics.Add(d).HasError())

	// query data for validate
	sql = "SELECT * FROM " + table.TableName
	queryResult, d := testCrudExecutor.Query(context.Background(), sql)
	assert.False(t, diagnostics.Add(d).HasError())
	rows, d := queryResult.ReadRows(1)
	assert.False(t, diagnostics.Add(d).HasError())
	row, err := rows.ToRow()
	assert.Nil(t, err)
	assert.Equal(t, int(row.GetIntOrDefault("id", -1)), id)
	assert.Equal(t, row.GetStringOrDefault("username", "nothing"), username)
	assert.Equal(t, int(row.GetIntOrDefault("age", -1)), age)

	assert.False(t, diagnostics.Add(testTableAdmin.TableDrop(context.Background(), table)).HasError())
}

func TestPostgresqlCRUDExecutor_Insert(t *testing.T) {
	diagnostics := schema.NewDiagnostics()

	// ensure test table exists and empty
	table := getTestTable()
	assert.False(t, diagnostics.Add(testTableAdmin.TableDrop(context.Background(), table)).HasError())
	assert.False(t, diagnostics.Add(testTableAdmin.TableCreate(context.Background(), table)).HasError())

	// test insert data
	rows := schema.NewRows("id", "username", "age")
	id := 1
	username := "Tom"
	age := 3
	assert.Nil(t, rows.AppendRowValues([]any{
		id, username, age,
	}))
	diagnostics.Add(testCrudExecutor.Insert(context.Background(), table, rows))
	t.Log(diagnostics.ToString())
	assert.False(t, diagnostics.HasError())

	// query data for validate
	sql := "SELECT * FROM " + table.TableName
	queryResult, d := testCrudExecutor.Query(context.Background(), sql)
	assert.False(t, diagnostics.Add(d).HasError())
	rows, d = queryResult.ReadRows(1)
	assert.False(t, diagnostics.Add(d).HasError())
	row, err := rows.ToRow()
	assert.Nil(t, err)
	assert.Equal(t, int(row.GetIntOrDefault("id", -1)), id)
	assert.Equal(t, row.GetStringOrDefault("username", "nothing"), username)
	assert.Equal(t, int(row.GetIntOrDefault("age", -1)), age)

	assert.False(t, diagnostics.Add(testTableAdmin.TableDrop(context.Background(), table)).HasError())
}
