package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_connectToPostgresqlServer(t *testing.T) {
	diagnostics := schema.NewDiagnostics()

	options := &PostgresqlStorageOptions{
		ConnectionString: env.GetDatabaseDsn(),
		SearchPath:       "fffffff",
	}
	pool, d := connectToPostgresqlServer(context.Background(), options)

	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	assert.NotNil(t, pool)
}
