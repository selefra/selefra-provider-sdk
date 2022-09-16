package postgresql_storage

import (
	"context"
	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"go.uber.org/zap"
)

type PostgresqlCRUDExecutor struct {
	pool       *pgxpool.Pool
	clientMeta *schema.ClientMeta
}

func (x *PostgresqlCRUDExecutor) SetClientMeta(clientMeta *schema.ClientMeta) {
	x.clientMeta = clientMeta
}

var _ storage.CRUDExecutor = &PostgresqlCRUDExecutor{}
var _ storage.UseClientMeta = &PostgresqlCRUDExecutor{}

func NewPostgresqlCRUDExecutor(pool *pgxpool.Pool) *PostgresqlCRUDExecutor {
	return &PostgresqlCRUDExecutor{
		pool: pool,
	}
}

func (x *PostgresqlCRUDExecutor) Query(ctx context.Context, query string, args ...any) (storage.QueryResult, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()
	rows, err := x.pool.Query(ctx, query, args...)

	if err != nil {
		return nil, diagnostics.AddErrorMsg("Postgresql sql query %s exec error: %s", query, err.Error())
	}

	return &PostgresqlQueryResult{
		rows: rows,
	}, nil
}

func (x *PostgresqlCRUDExecutor) Exec(ctx context.Context, query string, args ...any) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	_, err := x.pool.Exec(ctx, query, args...)
	if err != nil {

		x.clientMeta.Error("Postgresql sql exec error", zap.String("sql", query), zap.Any("args", args))

		diagnostics.AddErrorMsg("Postgresql sql %s exec error: %s", err.Error(), query)
	} else {

		x.clientMeta.Debug("Postgresql sql exec", zap.String("sql", query), zap.Any("args", args))

	}
	return diagnostics
}

func (x *PostgresqlCRUDExecutor) Insert(ctx context.Context, table *schema.Table, rows *schema.Rows) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if rows.IsEmpty() {
		return diagnostics.AddErrorMsg("table %s insert error: rows is empty", table.TableName)
	}

	pgsql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	// The form of the compatible keyword
	columnNameSlice := make([]string, 0)
	for _, columnName := range rows.GetColumnNames() {
		columnNameSlice = append(columnNameSlice, "\""+columnName+"\"")
	}
	sqlStmt := pgsql.Insert(table.TableName).Columns(columnNameSlice...)
	for _, columnValue := range rows.GetMatrix() {
		sqlStmt = sqlStmt.Values(columnValue...)
	}
	s, args, err := sqlStmt.ToSql()
	if err != nil {
		return diagnostics.AddErrorMsg("table %s insert build sql error: %s", table.TableName, err.Error())
	}

	err = x.pool.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, s, args...)
		return err
	})
	if err != nil {

		// print data when occur error
		x.clientMeta.Error("postgresql_storage insert error", zap.String("table", table.TableName), zap.String("rows", rows.String()), zap.Error(err))

		diagnostics.AddErrorMsg("table %s insert transaction error: %s", table.TableName, err.Error())
	} else {

		x.clientMeta.Debug("postgresql_storage insert", zap.String("table", table.TableName), zap.String("rows", rows.String()), zap.Error(err))

	}

	return diagnostics
}
