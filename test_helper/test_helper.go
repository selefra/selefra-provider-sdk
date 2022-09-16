package test_helper

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"github.com/selefra/selefra-provider-sdk/provider"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
	"github.com/selefra/selefra-utils/pkg/json_util"
	"github.com/selefra/selefra-utils/pkg/pointer"
)

// TestPullTables Design a test tool to facilitate the development of the Provider
func TestPullTables(myProvider *provider.Provider, config, workspace string, pullTables ...string) {

	diagnostics := schema.NewDiagnostics()

	// init Provider
	initProviderRequest := &shard.ProviderInitRequest{
		Storage: &shard.Storage{
			Type:           0,
			StorageOptions: json_util.ToJsonBytes(postgresql_storage.NewPostgresqlStorageOptions(env.GetDatabaseDsn())),
		},
		Workspace:      &workspace,
		IsInstallInit:  pointer.TruePointer(),
		ProviderConfig: &config,
	}

	response, err := myProvider.Init(context.Background(), initProviderRequest)
	if err != nil {
		panic(diagnostics.AddFatal("init provider error: %s", err.Error()).ToString())
	}
	if diagnostics.AddDiagnostics(response.Diagnostics).HasError() {
		panic(diagnostics.ToString())
	}

	err = myProvider.PullTables(context.Background(), &shard.PullTablesRequest{
		Tables:        pullTables,
		MaxGoroutines: 1,
		Timeout:       1000 * 60 * 60,
	}, shard.NewFakeProviderServerSender())
	if err != nil {
		panic(diagnostics.AddFatal("provider pull table error: %s", err.Error()).ToString())
	}
}
