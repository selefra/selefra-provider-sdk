package postgresql_storage

import (
	"context"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres" // init postgres
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
)

type PostgresqlStorage struct {
	*PostgresqlCRUDExecutor
	*PostgresqlTransactionExecutor
	*PostgresqlTableAdmin
	*PostgresqlNamespaceAdmin

	pool       *pgxpool.Pool
	clientMeta *schema.ClientMeta
}

func (x *PostgresqlStorage) SetClientMeta(clientMeta *schema.ClientMeta) {
	x.clientMeta = clientMeta
	x.PostgresqlCRUDExecutor.SetClientMeta(clientMeta)
}

var _ storage.Storage = &PostgresqlStorage{}

func NewPostgresqlStorage(ctx context.Context, options *PostgresqlStorageOptions) (*PostgresqlStorage, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	pool, d := connectToPostgresqlServer(ctx, options.DSN)
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}

	postgresqlStorage := &PostgresqlStorage{
		PostgresqlCRUDExecutor:        NewPostgresqlCRUDExecutor(pool),
		PostgresqlTransactionExecutor: NewPostgresqlTransactionExecutor(pool),
	}
	postgresqlStorage.PostgresqlTableAdmin = NewPostgresqlTableAdmin(postgresqlStorage.PostgresqlCRUDExecutor)
	postgresqlStorage.PostgresqlNamespaceAdmin = NewPostgresqlNamespaceAdmin(postgresqlStorage.PostgresqlCRUDExecutor)
	return postgresqlStorage, nil
}

// GetStorageConnection Expose THE CONNECTIONS OF THE PG SO THAT THE UPPER LAYER CAN DIRECTLY MANIPULATE THE CONNECTIONS OF THE LOWER layer if they feel it is necessary
func (x *PostgresqlStorage) GetStorageConnection() any {
	return x.pool
}

func connectToPostgresqlServer(ctx context.Context, dsnURI string) (*pgxpool.Pool, *schema.Diagnostics) {
	poolCfg, err := pgxpool.ParseConfig(dsnURI)
	if err != nil {
		return nil, schema.NewDiagnosticsAddErrorMsg("PostgresqlStorage pgxpool.ParseConfig error: %s", err.Error())
	}
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {

		// TODO 2022-8-24 19:21:39 If there is no public, I will get an error and need to create one. Okay? Damn it...
		// Put tables under public
		_, err := conn.Exec(ctx, "SET search_path=public")
		if err != nil {
			return err
		}

		return nil
	}

	config, err := pgxpool.ConnectConfig(ctx, poolCfg)
	if err != nil {
		return nil, schema.NewDiagnosticsAddErrorMsg("PostgresqlStorage connect server error: %s", err.Error())
	}
	return config, nil
}
