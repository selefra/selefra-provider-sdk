package storage_factory

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegisteredCreateStorageFactory(t *testing.T) {

	f := func(ctx context.Context, options storage.CreateStorageOptions) (storage.Storage, *schema.Diagnostics) {
		diagnostics := schema.NewDiagnostics()
		pgStorageOptions, ok := options.(*postgresql_storage.PostgresqlStorageOptions)
		if !ok {
			return nil, diagnostics.AddErrorMsg("create PostgresqlStorage error, options must be *postgresql_storage.PostgresqlStorageOptions")
		}
		return postgresql_storage.NewPostgresqlStorage(ctx, pgStorageOptions)
	}

	diagnostics := RegisteredCreateStorageFactory(StorageTypePostgresql, f)
	assert.Equal(t, diagnostics.HasError(), true)
}
