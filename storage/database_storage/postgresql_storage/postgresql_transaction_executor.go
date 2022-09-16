package postgresql_storage

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
)

type PostgresqlTransactionExecutor struct {
	pool *pgxpool.Pool
	tx   pgx.Tx
}

var _ storage.TransactionExecutor = &PostgresqlTransactionExecutor{}

func NewPostgresqlTransactionExecutor(pool *pgxpool.Pool) *PostgresqlTransactionExecutor {
	return &PostgresqlTransactionExecutor{
		pool: pool,
	}
}

func (x *PostgresqlTransactionExecutor) Begin(ctx context.Context) (storage.TransactionExecutor, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	tx, err := x.pool.Begin(ctx)
	if err != nil {
		return nil, diagnostics.AddErrorMsg("pg transaction begin error: %s", err.Error())
	}
	return &PostgresqlTransactionExecutor{tx: tx}, diagnostics
}

func (x *PostgresqlTransactionExecutor) Rollback(ctx context.Context) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	err := x.tx.Rollback(ctx)
	if err != nil {
		diagnostics.AddErrorMsg("pg transaction rollback error: %s", err.Error())
	}
	return diagnostics
}

func (x *PostgresqlTransactionExecutor) Commit(ctx context.Context) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	err := x.tx.Commit(ctx)
	if err != nil {
		diagnostics.AddErrorMsg("pg transaction commit error: %s", err.Error())
	}
	return diagnostics
}
