package postgresql_storage

import (
	"encoding/json"
	"github.com/selefra/selefra-provider-sdk/storage"
)

type PostgresqlStorageOptions struct {
	ConnectionString string
	SearchPath       string
}

var _ storage.CreateStorageOptions = &PostgresqlStorageOptions{}

func (x *PostgresqlStorageOptions) ToJsonString() (string, error) {
	marshal, err := json.Marshal(x)
	if err != nil {
		return "", err
	}
	return string(marshal), nil
}

func (x *PostgresqlStorageOptions) FromJsonString(jsonString string) error {
	return json.Unmarshal([]byte(jsonString), x)
}

func NewPostgresqlStorageOptions(connectionString string) *PostgresqlStorageOptions {
	return &PostgresqlStorageOptions{
		ConnectionString: connectionString,
	}
}
