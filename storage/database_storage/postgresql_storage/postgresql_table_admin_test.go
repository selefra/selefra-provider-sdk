package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/stretchr/testify/assert"
	"testing"
)

func getTestTable() *schema.Table {
	return &schema.Table{
		TableName: "t_test_user",
		Options: &schema.TableOptions{
			PrimaryKeys: []string{
				"id",
			},
		},
		Columns: []*schema.Column{
			{
				ColumnName: "id",
				Type:       schema.ColumnTypeBigInt,
			},
			{
				ColumnName: "username",
				Type:       schema.ColumnTypeString,
			},
			{
				ColumnName: "age",
				Type:       schema.ColumnTypeSmallInt,
			},
		},
		SubTables: []*schema.Table{
			&schema.Table{
				TableName: "t_test_user_visit_log",
				Options: &schema.TableOptions{
					PrimaryKeys: []string{
						"id",
					},
					ForeignKeys: []*schema.TableForeignKey{
						{
							SelfColumns:      []string{"user_id"},
							ForeignTableName: "t_test_user",
							ForeignColumns: []string{
								"id",
							},
						},
					},
				},
				Columns: []*schema.Column{
					{
						ColumnName: "id",
						Type:       schema.ColumnTypeBigInt,
					},
					{
						ColumnName: "user_id",
						Type:       schema.ColumnTypeBigInt,
						Extractor:  column_value_extractor.ParentPrimaryKeysID(),
					},
					{
						ColumnName: "age",
						Type:       schema.ColumnTypeSmallInt,
					},
				},
			},
		},
	}
}

func TestPostgresqlTableAdmin_TableList(t *testing.T) {
	tableList, diagnostics := testTableAdmin.TableList(context.Background(), "public")
	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	for _, table := range tableList {
		t.Log(table.TableName)
		for _, column := range table.Columns {
			t.Logf("        %s", column.ColumnName)
		}
	}
}

func TestPostgresqlTableAdmin_TableCreate(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	diagnostics.Add(testTableAdmin.TableCreate(context.Background(), table))
	assert.False(t, diagnostics.HasError())
}

func TestPostgresqlTableAdmin_TablesCreate(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	diagnostics.Add(testTableAdmin.TablesCreate(context.Background(), []*schema.Table{table}))
	assert.False(t, diagnostics.HasError())
}

func TestPostgresqlTableAdmin_buildCreateTableSqlSlice(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	sqlSlice, d := testTableAdmin.buildCreateTableSqlSlice(context.Background(), table)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.True(t, len(sqlSlice) == 2)
	assert.Equal(t, sqlSlice[0], "CREATE TABLE IF NOT EXISTS \"t_test_user\" ( \n   \"id\" bigint ,  \n  \"username\" text ,  \n  \"age\" smallint   \n); ")
	assert.Equal(t, sqlSlice[1], "CREATE TABLE IF NOT EXISTS \"t_test_user_visit_log\" ( \n   \"id\" bigint ,  \n  \"user_id\" bigint ,  \n  \"age\" smallint   \n); ")
}

func TestPostgresqlTableAdmin_buildCreateTableConstraintSql(t *testing.T) {
	
}

func TestPostgresqlTableAdmin_isConstraintExists(t *testing.T) {

}

func TestPostgresqlTableAdmin_TableDrop(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	diagnostics.Add(testTableAdmin.TableDrop(context.Background(), table))
	assert.False(t, diagnostics.HasError())
}

func TestPostgresqlTableAdmin_TablesDrop(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	diagnostics.Add(testTableAdmin.TablesDrop(context.Background(), []*schema.Table{table}))
	assert.False(t, diagnostics.HasError())
}

func TestPostgresqlTableAdmin_buildDropTableConstraintSql(t *testing.T) {

}

func TestPostgresqlTableAdmin_buildDropTableSqlSlice(t *testing.T) {
	diagnostics := schema.NewDiagnostics()
	table := getTestTable()
	sqlSlice, d := testTableAdmin.buildDropTableSqlSlice(context.Background(), table)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.True(t, len(sqlSlice) == 2)
	assert.Equal(t, sqlSlice[0], "DROP TABLE IF EXISTS t_test_user")
	assert.Equal(t, sqlSlice[1], "DROP TABLE IF EXISTS t_test_user_visit_log")
}
