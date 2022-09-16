package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-utils/pkg/string_util"
)

type PostgresqlNamespaceAdmin struct {
	crudExecutor storage.CRUDExecutor
}

var _ storage.NamespaceAdmin = &PostgresqlNamespaceAdmin{}

func NewPostgresqlNamespaceAdmin(crudExecutor storage.CRUDExecutor) *PostgresqlNamespaceAdmin {
	return &PostgresqlNamespaceAdmin{
		crudExecutor: crudExecutor,
	}
}

func (x *PostgresqlNamespaceAdmin) NamespaceList(ctx context.Context) ([]string, *schema.Diagnostics) {
	// TODO
	//diagnostics := schema.NewDiagnostics()
	//
	//sql := "select schema_name from information_schema.schemata"
	//queryResult, d := x.crudExecutor.Query(ctx, sql)
	//if diagnostics.Add(d).HasError() {
	//	return nil, diagnostics
	//}
	//rows, d := queryResult.ReadRows(-1)
	//
	//schemaNameSlice := make([]string, 0)
	//for queryResult.Next() {
	//	valuesMap, d := queryResult.ValuesMap()
	//	if diagnostics.Add(d).HasError() {
	//		return nil, diagnostics
	//	}
	//	if valuesMap["schema_name"] != "" {
	//		schemaNameSlice = append(schemaNameSlice, valuesMap["schema_name"])
	//	}
	//}
	//return
	return nil, nil
}

func (x *PostgresqlNamespaceAdmin) NamespaceCreate(ctx context.Context, namespace string) *schema.Diagnostics {
	sql := string_util.NewStringBuilder()
	sql.WriteString("CREATE SCHEMA ").WriteString(namespace).WriteString("; ")
	//sql.WriteString(" WITH ENCODING 'UTF8' ").
	//	WriteString(" LC_COLLATE = 'en_US.UTF-8 ").
	//	WriteString(" LC_CTYPE = 'en_US.UTF-8' ").
	//	WriteString(" TEMPLATE template0;")
	return x.crudExecutor.Exec(ctx, sql.String())
}

func (x *PostgresqlNamespaceAdmin) NamespaceDrop(ctx context.Context, namespace string) *schema.Diagnostics {
	sql := string_util.NewStringBuilder()
	sql.WriteString("DROP SCHEMA ").WriteString(namespace).WriteString("; ")
	return x.crudExecutor.Exec(ctx, sql.String())
}
