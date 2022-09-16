package postgresql_storage

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// Convert Responsible for converting standard column types to their Postgresql counterparts
func getColumnPgType(table *schema.Table, column *schema.Column) (string, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	switch column.Type {

	case schema.ColumnTypeSmallInt:
		return "smallint", diagnostics
	case schema.ColumnTypeInt:
		return "integer", diagnostics
	case schema.ColumnTypeIntArray:
		return "integer[]", diagnostics
	case schema.ColumnTypeBigInt:
		return "bigint", diagnostics

	case schema.ColumnTypeFloat:
		return "float", diagnostics

	case schema.ColumnTypeBool:
		return "boolean", diagnostics

	case schema.ColumnTypeString:
		return "text", diagnostics
	case schema.ColumnTypeStringArray:
		return "text[]", diagnostics

	case schema.ColumnTypeByteArray:
		return "bytea", diagnostics

	case schema.ColumnTypeTimestamp:
		return "timestamp without time zone", diagnostics

	case schema.ColumnTypeJSON:
		return "jsonb", diagnostics

	case schema.ColumnTypeIp:
		return "inet", diagnostics
	case schema.ColumnTypeIpArray:
		return "inet[]", diagnostics

	case schema.ColumnTypeCIDR:
		return "cidr", diagnostics
	case schema.ColumnTypeCIDRArray:
		return "cidr[]", diagnostics

	case schema.ColumnTypeMacAddr:
		return "mac", diagnostics
	case schema.ColumnTypeMacAddrArray:
		return "mac[]", diagnostics

	case schema.ColumnTypeNotAssign:
		return "", diagnostics.AddErrorMsg("PostgresqlColumnTypeConvertor table %s column %s not assign type", table.TableName, column.ColumnName)
	default:
		return "", diagnostics.AddErrorMsg("PostgresqlColumnTypeConvertor table %s column %s type unknown: %s", table.TableName, column.ColumnName, column.Type.String())
	}
}
