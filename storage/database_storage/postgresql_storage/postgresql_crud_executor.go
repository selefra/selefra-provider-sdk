package postgresql_storage

import (
	"context"
	"errors"
	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"go.uber.org/zap"
	"time"
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

	startTime := time.Now()
	rows, err := x.pool.Query(ctx, query, args...)
	cost := time.Now().Sub(startTime)

	if err != nil {
		if x.clientMeta != nil {
			x.clientMeta.Error("Postgresql sql query query error", zap.String("sql", query), zap.String("cost", cost.String()), zap.Error(err))
		}
		return nil, diagnostics.AddErrorMsg("Postgresql sql query %s exec error: %s", query, err.Error())
	}
	if x.clientMeta != nil {
		x.clientMeta.Debug("Postgresql sql query query success", zap.String("sql", query), zap.String("cost", cost.String()))
	}

	return &PostgresqlQueryResult{
		rows: rows,
	}, nil
}

func (x *PostgresqlCRUDExecutor) Exec(ctx context.Context, query string, args ...any) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()

	startTime := time.Now()
	_, err := x.pool.Exec(ctx, query, args...)
	cost := time.Now().Sub(startTime)

	if err != nil {
		if x.clientMeta != nil {
			x.clientMeta.Error("Postgresql sql exec error", zap.String("sql", query), zap.String("cost", cost.String()), zap.Error(err))
		}
		diagnostics.AddErrorMsg("Postgresql sql %s exec error: %s", err.Error(), query)
	}
	if x.clientMeta != nil {
		x.clientMeta.Debug("Postgresql sql exec success", zap.String("sql", query), zap.String("cost", cost.String()))
	}
	return diagnostics
}

func (x *PostgresqlCRUDExecutor) Insert(ctx context.Context, table *schema.Table, rows *schema.Rows) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if rows.IsEmpty() {
		if x.clientMeta != nil {
			x.clientMeta.Error("postgresql_storage insert error 001, because want insert empty row", zap.String("table", table.TableName))
		}
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
		if x.clientMeta != nil {
			x.clientMeta.Error("postgresql_storage insert error 002", zap.String("table", table.TableName), zap.String("rows", rows.String()), zap.Error(err))
		}
		return diagnostics.AddErrorMsg("table %s insert build sql error: %s", table.TableName, err.Error())
	}

	startTime := time.Now()
	err = x.pool.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.ReadCommitted,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, s, args...)
		return err
	})
	cost := time.Now().Sub(startTime)
	if err != nil {
		if x.clientMeta != nil {
			x.clientMeta.Error("postgresql_storage insert error 003", zap.String("table", table.TableName), zap.String("rows", rows.String()), zap.String("cost", cost.String()), zap.Error(err))
		}
		diagnostics.AddErrorMsg("table %s insert transaction error: %s", table.TableName, err.Error())
	} else {
		if x.clientMeta != nil {
			x.clientMeta.Debug("postgresql_storage insert success", zap.String("table", table.TableName), zap.String("rows", rows.String()), zap.String("cost", cost.String()))
		}
	}

	return diagnostics
}

func (x *PostgresqlStorage) GetTime(ctx context.Context) (time.Time, error) {
	var zero time.Time
	sql := `SELECT NOW()`
	rs, err := x.pool.Query(ctx, sql)
	if err != nil {
		return zero, err
	}
	defer func() {
		rs.Close()
	}()
	if !rs.Next() {
		return zero, errors.New("can not query database time")
	}
	var dbTime time.Time
	err = rs.Scan(&dbTime)
	if err != nil {
		return zero, err
	}
	return dbTime, nil
}
