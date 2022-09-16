package postgresql_storage

import "github.com/selefra/selefra-provider-sdk/provider/schema"

func (x *PostgresqlStorage) Close() *schema.Diagnostics {
	if x.pool != nil {
		x.pool.Close()
	}
	return nil
}
