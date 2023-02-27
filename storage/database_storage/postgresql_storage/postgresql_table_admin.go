package postgresql_storage

import (
	"context"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-utils/pkg/string_util"
	"github.com/spf13/cast"
	"strconv"
	"strings"
)

type PostgresqlTableAdmin struct {
	crudExecutor storage.CRUDExecutor
}

var _ storage.TableAdmin = &PostgresqlTableAdmin{}

func NewPostgresqlTableAdmin(crudExecutor storage.CRUDExecutor) *PostgresqlTableAdmin {
	return &PostgresqlTableAdmin{
		crudExecutor: crudExecutor,
	}
}

// TableList List all the tables under PG, here considering that the table may be very many, so the enumeration is concurrent
func (x *PostgresqlTableAdmin) TableList(ctx context.Context, namespace string) ([]*schema.Table, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	sql := "select table_schema, table_name, column_name from information_schema.columns where table_schema=$1 and table_name<>'pg_stat_statements'"
	queryResult, d := x.crudExecutor.Query(ctx, sql, namespace)
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}

	tableNameToTableMap := make(map[string]*schema.Table, 0)
	for queryResult.Next() {
		valuesMap, d := queryResult.ValuesMap()
		if diagnostics.AddDiagnostics(d).HasError() {
			return nil, diagnostics
		}

		namespace, err := cast.ToStringE(valuesMap["table_schema"])
		if err != nil {
			return nil, diagnostics.AddErrorMsg("TableList error: %s", err.Error())
		}

		tableName, err := cast.ToStringE(valuesMap["table_name"])
		if err != nil {
			return nil, diagnostics.AddErrorMsg("TableList error: %s", err.Error())
		}

		columnName, err := cast.ToStringE(valuesMap["column_name"])
		if err != nil {
			return nil, diagnostics.AddErrorMsg("TableList error: %s", err.Error())
		}

		table := tableNameToTableMap[tableName]
		if table == nil {
			table = &schema.Table{}
		}

		table.Runtime().Namespace = namespace
		table.TableName = tableName

		table.Columns = append(table.Columns, &schema.Column{
			ColumnName: columnName,
		})

		tableNameToTableMap[tableName] = table
	}

	// convert to slice for return
	tableSlice := make([]*schema.Table, 0)
	for _, table := range tableNameToTableMap {
		tableSlice = append(tableSlice, table)
	}
	return tableSlice, diagnostics
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *PostgresqlTableAdmin) TableCreate(ctx context.Context, table *schema.Table) *schema.Diagnostics {
	return x.TablesCreate(ctx, []*schema.Table{table})
}

func (x *PostgresqlTableAdmin) TablesCreate(ctx context.Context, tables []*schema.Table) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	createTableSqlSlice := make([]string, 0)
	addConstraintsSqlSlice := make([]string, 0)
	for _, table := range tables {

		sqlSlice, d := x.buildCreateTableSqlSlice(ctx, table)
		if !diagnostics.AddDiagnostics(d).HasError() {
			createTableSqlSlice = append(createTableSqlSlice, sqlSlice...)
		}

		sqlSlice, d = x.buildCreateTableConstraintSql(ctx, table)
		if !diagnostics.AddDiagnostics(d).HasError() {
			addConstraintsSqlSlice = append(addConstraintsSqlSlice, sqlSlice...)
		}

	}

	sqlSlice := make([]string, 0)
	sqlSlice = append(sqlSlice, createTableSqlSlice...)
	sqlSlice = append(sqlSlice, addConstraintsSqlSlice...)
	sqlSet := make(map[string]struct{}, 0)
	for _, sql := range sqlSlice {
		if _, exists := sqlSet[sql]; exists {
			continue
		}
		sqlSet[sql] = struct{}{}
		// just exec all sql
		diagnostics.AddDiagnostics(x.crudExecutor.Exec(ctx, sql))
	}
	return diagnostics
}

func (x *PostgresqlTableAdmin) buildCreateTableSqlSlice(ctx context.Context, table *schema.Table) ([]string, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	createTableSqlSlice := make([]string, 0)

	sql := string_util.NewStringBuilder()
	sql.WriteString("CREATE TABLE IF NOT EXISTS ").
		WriteString(strconv.Quote(table.TableName)).
		WriteString(" ( \n ")

	for index, column := range table.Columns {

		s, convertorDiagnostics := GetColumnPostgreSQLType(table, column)
		if diagnostics.AddDiagnostics(convertorDiagnostics).HasError() {
			return nil, diagnostics
		}
		sql.WriteString(fmt.Sprintf("  \"%s\" %s ", column.ColumnName, s))

		if column.Options.NotNull != nil && *column.Options.NotNull {
			sql.WriteString(" NOT NULL ")
		}

		if column.Options.Unique != nil && *column.Options.Unique {
			sql.WriteString(" UNIQUE ")
		}

		if index < len(table.Columns)-1 {
			sql.WriteString(",")
		}

		sql.WriteString("  \n")
	}
	sql.WriteString("); ")
	createTableSqlSlice = append(createTableSqlSlice, sql.String())

	for _, subTable := range table.SubTables {
		subTableSqlSlice, d := x.buildCreateTableSqlSlice(ctx, subTable)
		if !diagnostics.AddDiagnostics(d).HasError() {
			createTableSqlSlice = append(createTableSqlSlice, subTableSqlSlice...)
		}
	}

	return createTableSqlSlice, diagnostics
}

func (x *PostgresqlTableAdmin) buildCreateTableConstraintSql(ctx context.Context, table *schema.Table) ([]string, *schema.Diagnostics) {

	sqlSlice := make([]string, 0)
	diagnostics := schema.NewDiagnostics()

	if table.Options != nil {

		// pk
		if table.Options.PrimaryKeys != nil {
			pkName := table.Options.GenPrimaryKeysName(table.TableName)
			exists, d := x.isConstraintExists(ctx, pkName)
			if diagnostics.AddDiagnostics(d).HasError() {
				return sqlSlice, diagnostics
			}
			if !exists {
				sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s);", table.TableName, pkName, strings.Join(table.Options.PrimaryKeys, ", "))
				sqlSlice = append(sqlSlice, sql)
			}
		}

		// fk
		if len(table.Options.ForeignKeys) != 0 {
			for _, fk := range table.Options.ForeignKeys {
				fkName := fk.GetName(table.TableName)
				exists, d := x.isConstraintExists(ctx, fkName)
				if diagnostics.AddDiagnostics(d).HasError() {
					return sqlSlice, diagnostics
				}
				if !exists {
					sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);", table.TableName, fkName, strings.Join(fk.SelfColumns, ", "), fk.ForeignTableName, strings.Join(fk.ForeignColumns, ", "))
					sqlSlice = append(sqlSlice, sql)
				}
			}
		}

		// index
		if len(table.Options.Indexes) != 0 {
			for _, idx := range table.Options.Indexes {
				idxName := idx.GetName(table.TableName)
				exists, d := x.isConstraintExists(ctx, idxName)
				if diagnostics.AddDiagnostics(d).HasError() {
					return sqlSlice, diagnostics
				}
				if !exists {
					sql := strings.Builder{}
					sql.WriteString("CREATE ")
					if idx.IsUniq != nil && *idx.IsUniq {
						sql.WriteString(" UNIQUE  ")
					}
					sql.WriteString(" INDEX ")
					sql.WriteString(idxName)
					sql.WriteString(" ON ")
					sql.WriteString(table.TableName)
					sql.WriteString(" ( ")
					sql.WriteString(strings.Join(idx.ColumnNames, ", "))
					sql.WriteString(" ) ")
					sqlSlice = append(sqlSlice, sql.String())
				}
			}
		}

	}

	// sub tables
	for _, subTable := range table.SubTables {
		sql, d := x.buildCreateTableConstraintSql(ctx, subTable)
		if !diagnostics.AddDiagnostics(d).HasError() {
			sqlSlice = append(sqlSlice, sql...)
		}
	}

	return sqlSlice, diagnostics
}

func (x *PostgresqlTableAdmin) isConstraintExists(ctx context.Context, constraintName string) (bool, *schema.Diagnostics) {
	sql := fmt.Sprintf("SELECT 1 FROM pg_constraint WHERE conname = '%s'", constraintName)
	query, diagnostics := x.crudExecutor.Query(ctx, sql)
	defer func() {
		if query != nil {
			query.Close()
		}
	}()
	if diagnostics != nil && diagnostics.HasError() {
		return false, diagnostics
	}
	rows, d := query.ReadRows(1)
	if d != nil && d.HasError() {
		return false, d
	}
	return rows.RowCount() == 1, nil
}

func (x *PostgresqlTableAdmin) TableDrop(ctx context.Context, table *schema.Table) *schema.Diagnostics {
	return x.TablesDrop(ctx, []*schema.Table{table})
}

func (x *PostgresqlTableAdmin) TablesDrop(ctx context.Context, tables []*schema.Table) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()
	dropTableConstraintSqlSlice := make([]string, 0)
	dropTableSqlSlice := make([]string, 0)

	for _, table := range tables {

		sqlSlice, d := x.buildDropTableConstraintSql(ctx, table)
		if !diagnostics.AddDiagnostics(d).HasError() {
			dropTableConstraintSqlSlice = append(dropTableConstraintSqlSlice, sqlSlice...)
		}

		sqlSlice, d = x.buildDropTableSqlSlice(ctx, table)
		if !diagnostics.AddDiagnostics(d).HasError() {
			dropTableSqlSlice = append(dropTableSqlSlice, sqlSlice...)
		}

	}

	if diagnostics.HasError() {
		return diagnostics
	}

	sqlSlice := make([]string, 0)
	sqlSlice = append(sqlSlice, dropTableConstraintSqlSlice...)
	sqlSlice = append(sqlSlice, dropTableSqlSlice...)
	sqlSet := make(map[string]struct{})
	for _, sql := range sqlSlice {
		if _, exists := sqlSet[sql]; exists {
			continue
		}
		sqlSet[sql] = struct{}{}
		// just exec all sql
		diagnostics.AddDiagnostics(x.crudExecutor.Exec(ctx, sql))
	}
	return diagnostics
}

func (x *PostgresqlTableAdmin) buildDropTableConstraintSql(ctx context.Context, table *schema.Table) ([]string, *schema.Diagnostics) {

	sqlSlice := make([]string, 0)
	diagnostics := schema.NewDiagnostics()

	for _, subTable := range table.SubTables {
		sql, d := x.buildDropTableConstraintSql(ctx, subTable)
		if !diagnostics.AddDiagnostics(d).HasError() {
			sqlSlice = append(sqlSlice, sql...)
		}
	}

	if table.Options != nil {
		if len(table.Options.ForeignKeys) != 0 {
			for _, fk := range table.Options.ForeignKeys {
				fkName := fk.GetName(table.TableName)
				exists, d := x.isConstraintExists(ctx, fkName)
				if diagnostics.AddDiagnostics(d).HasError() {
					return sqlSlice, diagnostics
				}
				if exists {
					sql := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", table.TableName, fkName)
					sqlSlice = append(sqlSlice, sql)
				}
			}
		}

	}

	return sqlSlice, diagnostics
}

func (x *PostgresqlTableAdmin) buildDropTableSqlSlice(ctx context.Context, table *schema.Table) ([]string, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()
	sqlSlice := make([]string, 0)

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", table.TableName)
	sqlSlice = append(sqlSlice, sql)

	for _, subTable := range table.SubTables {
		sql, d := x.buildDropTableSqlSlice(ctx, subTable)
		if !diagnostics.AddDiagnostics(d).HasError() {
			sqlSlice = append(sqlSlice, sql...)
		}
	}

	return sqlSlice, diagnostics
}
