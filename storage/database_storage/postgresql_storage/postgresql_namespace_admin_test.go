package postgresql_storage

import (
	"testing"
)

func TestPostgresqlNamespaceAdmin(t *testing.T) {

	// TODO
	//// create namespace
	//diagnostics := schema.NewDiagnostics()
	//namespace := "c1d29fe4ec649cab6916c93f44711bec"
	//assert.False(t, diagnostics.Add(testNamespaceAdmin.NamespaceCreate(context.Background(), namespace)).HasError())
	//
	//// list namespace for check create success
	//namespaceSlice, d := testNamespaceAdmin.NamespaceList(context.Background())
	//assert.False(t, diagnostics.Add(d).HasError())
	//for _, namespace := range namespaceSlice {
	//}
	//
	//// drop namespace
	//
	//// list namespace for check drop success

}

func TestPostgresqlNamespaceAdmin_NamespaceDrop(t *testing.T) {

}

func TestPostgresqlNamespaceAdmin_NamespaceList(t *testing.T) {

	// create namespace

	//namespaceSlice, diagnostics := testNamespaceAdmin.NamespaceList(context.Background())
	//assert.True(t, diagnostics == nil || !diagnostics.HasError())
}
