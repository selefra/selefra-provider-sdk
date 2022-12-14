package postgresql_storage

import (
	"context"
	"fmt"
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
var testPostgresqlStorage *PostgresqlStorage

func TestMain(m *testing.M) {
	diagnostics := schema.NewDiagnostics()

	workspace := "."
	clientMeta := schema.ClientMeta{}
	clientMetaRuntime, d := schema.NewClientMetaRuntime(context.Background(), workspace, "test", "v0.0.1", &clientMeta, nil, true)
	if diagnostics.Add(d).HasError() {
		panic(diagnostics.ToString())
	}
	_ = reflect_util.SetStructPtrUnExportedStrField(&clientMeta, "runtime", clientMetaRuntime)

	dsn := env.GetDatabaseDsn()
	fmt.Println("Test Use Database: " + dsn)
	pool, d := connectToPostgresqlServer(context.Background(), &PostgresqlStorageOptions{
		ConnectionString: dsn,
		SearchPath:       "",
	})
	assert.True(nil, d == nil || !d.HasError())

	testCrudExecutor = NewPostgresqlCRUDExecutor(pool)
	testCrudExecutor.SetClientMeta(&clientMeta)

	testKeyValueExecutor = NewPostgresqlKeyValueExecutor(testCrudExecutor)

	testTableAdmin = NewPostgresqlTableAdmin(testCrudExecutor)
	testNamespaceAdmin = NewPostgresqlNamespaceAdmin(testCrudExecutor)

	testPostgresqlStorage, d = NewPostgresqlStorage(context.Background(), NewPostgresqlStorageOptions(env.GetDatabaseDsn()))
	if diagnostics.Add(d).HasError() {
		panic(diagnostics.ToString())
	}

	code := m.Run()
	os.Exit(code)

}
