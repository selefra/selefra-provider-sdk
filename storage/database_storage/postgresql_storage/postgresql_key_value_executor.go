package postgresql_storage

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
)

type PostgresqlKeyValueExecutor struct {
	executor   *PostgresqlCRUDExecutor
	clientMeta *schema.ClientMeta
}

var _ storage.KeyValueExecutor = &PostgresqlKeyValueExecutor{}

func NewPostgresqlKeyValueExecutor(executor *PostgresqlCRUDExecutor) *PostgresqlKeyValueExecutor {
	return &PostgresqlKeyValueExecutor{
		executor: executor,
	}
}

func ensureKeyValueTableExists(ctx context.Context, conn *pgx.Conn) {
	createTableSql := `CREATE TABLE IF NOT EXISTS selefra_meta_kv (
			"key" text UNIQUE,
			value text 
		)`
	_, _ = conn.Exec(ctx, createTableSql)
}

func (x *PostgresqlKeyValueExecutor) SetKey(ctx context.Context, key, value string) *schema.Diagnostics {
	sql := `INSERT INTO selefra_meta_kv (
                             "key",
                             "value" 
                             ) VALUES ( $1, $2 ) ON CONFLICT (key) DO UPDATE SET value = $3`
	return x.executor.Exec(ctx, sql, key, value, value)
}

func (x *PostgresqlKeyValueExecutor) GetValue(ctx context.Context, key string) (string, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	sql := `SELECT value FROM selefra_meta_kv WHERE key = $1`
	query, d := x.executor.Query(ctx, sql, key)
	if diagnostics.AddDiagnostics(d).HasError() {
		return "", diagnostics
	}
	defer func() {
		if query != nil {
			query.Close()
		}
	}()
	if !query.Next() {
		return "", nil
	}

	var value string
	if diagnostics.AddDiagnostics(query.Decode(&value)).HasError() {
		return "", diagnostics
	}

	return value, nil
}

func (x *PostgresqlKeyValueExecutor) DeleteKey(ctx context.Context, key string) *schema.Diagnostics {
	sql := `DELETE FROM selefra_meta_kv WHERE key = $1`
	return x.executor.Exec(ctx, sql, key)
}

func (x *PostgresqlKeyValueExecutor) ListKey(ctx context.Context) (*schema.Rows, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	sql := `SELECT key, value FROM selefra_meta_kv`
	queryResult, d := x.executor.Query(ctx, sql)
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}
	defer func() {
		if queryResult != nil {
			queryResult.Close()
		}
	}()
	return queryResult.ReadRows(-1)
}
