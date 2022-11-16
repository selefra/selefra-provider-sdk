package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var testCrudExecutor *PostgresqlCRUDExecutor
var testKeyValueExecutor *PostgresqlKeyValueExecutor
var testTableAdmin *PostgresqlTableAdmin
var testNamespaceAdmin *PostgresqlNamespaceAdmin

func TestMain(m *testing.M) {
	diagnostics := schema.NewDiagnostics()

	workspace := "."
	clientMeta := schema.ClientMeta{}
	clientMetaRuntime, d := schema.NewClientMetaRuntime(context.Background(), workspace, "test", &clientMeta, nil, true)
	if diagnostics.Add(d).HasError() {
		panic(diagnostics.ToString())
	}
	_ = reflect_util.SetStructPtrUnExportedStrField(&clientMeta, "runtime", clientMetaRuntime)

	pool, diagnostics := connectToPostgresqlServer(context.Background(), &PostgresqlStorageOptions{
		ConnectionString: env.GetDatabaseDsn(),
		SearchPath:       "",
	})
	assert.True(nil, diagnostics == nil || !diagnostics.HasError())

	testCrudExecutor = NewPostgresqlCRUDExecutor(pool)
	testCrudExecutor.SetClientMeta(&clientMeta)

	testKeyValueExecutor = NewPostgresqlKeyValueExecutor(testCrudExecutor)

	testTableAdmin = NewPostgresqlTableAdmin(testCrudExecutor)
	testNamespaceAdmin = NewPostgresqlNamespaceAdmin(testCrudExecutor)

	code := m.Run()
	os.Exit(code)

}
