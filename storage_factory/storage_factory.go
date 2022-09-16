package storage_factory

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
)

// ------------------------------------------------- Supported storage media types -------------------------------------

type StorageType int

const (

	// StorageTypePostgresql postgresql
	StorageTypePostgresql StorageType = iota

	//// StorageTypeMySQL mysql
	//StorageTypeMySQL
	//
	//StorageTypeTidb
	//
	//StorageTypeSqlite
)

func (x StorageType) String() string {
	switch x {
	case StorageTypePostgresql:
		return "Postgres"
	//case StorageTypeMySQL:
	//	return "MySQL"
	//case StorageTypeTidb:
	//	return "TiDB"
	//case StorageTypeSqlite:
	//	return "Sqlite"
	default:
		return "unknown"
	}
}

// ------------------------------------------------- The factory method  ------------------------------------------------------------------------

// CreateStorageFactoryFunction The factory method that creates the Storage
type CreateStorageFactoryFunction func(ctx context.Context, options storage.CreateStorageOptions) (storage.Storage, *schema.Diagnostics)

// Key is the storage type, and the value is the constructor that creates the corresponding storage type
var storageFactoryMethodMap = make(map[StorageType]CreateStorageFactoryFunction)

// Initialize the databases supported by the registration
func init() {

	// Register the factory function for PostgresqlStorage
	diagnostics := RegisteredCreateStorageFactory(StorageTypePostgresql, func(ctx context.Context, options storage.CreateStorageOptions) (storage.Storage, *schema.Diagnostics) {
		diagnostics := schema.NewDiagnostics()
		pgStorageOptions, ok := options.(*postgresql_storage.PostgresqlStorageOptions)
		if !ok {
			return nil, diagnostics.AddErrorMsg("create PostgresqlStorage error, options must be *postgresql_storage.PostgresqlStorageOptions")
		}
		return postgresql_storage.NewPostgresqlStorage(ctx, pgStorageOptions)
	})
	if diagnostics != nil && diagnostics.HasError() {
		panic(diagnostics.ToString())
		return
	}

	// TODO Other types of media are implemented and registered here


}

func RegisteredCreateStorageFactory(storageType StorageType, function CreateStorageFactoryFunction) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	// Do not allow the same type to be registered more than once, otherwise it may be overwritten and become a different implementation
	if _, exists := storageFactoryMethodMap[storageType]; exists {
		return diagnostics.AddErrorMsg("storage can not be registered because it is already registered, do not allow replication of storage")
	}

	// Save the factory function if the check passes
	storageFactoryMethodMap[storageType] = function

	return diagnostics
}

// NewStorage Uses the passed arguments to create a Storage of a given type
func NewStorage(ctx context.Context, storageType StorageType, options storage.CreateStorageOptions) (storage.Storage, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()
	function, exists := storageFactoryMethodMap[storageType]
	if !exists {
		return nil, diagnostics.AddErrorMsg("storage type %s not found", storageType.String())
	}
	return function(ctx, options)
}
