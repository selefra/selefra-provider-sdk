package postgresql_storage

import (
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

func (x *PostgresqlStorage) NewColumnValueConvertor() schema.ColumnValueConvertor {
	// use default type convertor
	return nil
}
