package storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"time"
)

// Storage Represents a storage medium, which may be implemented in many different ways
type Storage interface {

	// NamespaceAdmin Be able to manage namespaces, where tables are stored
	NamespaceAdmin

	// TableAdmin Be able to manage tables for the underlying storage
	TableAdmin

	// CRUDExecutor You can perform operations such as adding, deleting, modifying and querying on storage media
	CRUDExecutor

	// TransactionExecutor Transactional operations can be performed on storage media
	TransactionExecutor

	// Closeable Storage media can be turned off
	Closeable

	// ColumnValueConvertorFactory This storage medium can convert incoming data to its own storage medium's corresponding type
	ColumnValueConvertorFactory

	// GetStorageConnection Allows the connection to a specific Storage to be exposed upward, which is used if you want to work directly with the underlying Storage
	GetStorageConnection() any

	UseClientMeta

	KeyValueExecutor

	Lock

	TimeProvider
}

// TimeProvider Acquired time
type TimeProvider interface {

	// GetTime In a distributed system, use a uniform date
	GetTime(ctx context.Context) (time.Time, error)
}

type UseClientMeta interface {
	SetClientMeta(clientMeta *schema.ClientMeta)
}

type ColumnValueConvertorFactory interface {
	NewColumnValueConvertor() schema.ColumnValueConvertor
}

type TableAdmin interface {
	TableCreate(ctx context.Context, table *schema.Table) *schema.Diagnostics

	TablesCreate(ctx context.Context, tables []*schema.Table) *schema.Diagnostics

	TableDrop(ctx context.Context, table *schema.Table) *schema.Diagnostics

	TablesDrop(ctx context.Context, tables []*schema.Table) *schema.Diagnostics

	TableList(ctx context.Context, namespace string) ([]*schema.Table, *schema.Diagnostics)
}

type NamespaceAdmin interface {
	NamespaceList(ctx context.Context) ([]string, *schema.Diagnostics)
	NamespaceCreate(ctx context.Context, namespace string) *schema.Diagnostics
	NamespaceDrop(ctx context.Context, namespace string) *schema.Diagnostics
}

type Closeable interface {
	Close() *schema.Diagnostics
}
type TransactionExecutor interface {
	Begin(ctx context.Context) (TransactionExecutor, *schema.Diagnostics)
	Rollback(ctx context.Context) *schema.Diagnostics
	Commit(ctx context.Context) *schema.Diagnostics
}

type CRUDExecutor interface {
	Query(ctx context.Context, query string, args ...any) (QueryResult, *schema.Diagnostics)

	Exec(ctx context.Context, query string, args ...any) *schema.Diagnostics

	Insert(ctx context.Context, t *schema.Table, rowSet *schema.Rows) *schema.Diagnostics
}

type KeyValueExecutor interface {
	SetKey(ctx context.Context, key, value string) *schema.Diagnostics

	GetValue(ctx context.Context, key string) (string, *schema.Diagnostics)

	DeleteKey(ctx context.Context, key string) *schema.Diagnostics

	ListKey(ctx context.Context) (*schema.Rows, *schema.Diagnostics)
}

type Lock interface {
	Lock(ctx context.Context, lockId, ownerId string) error
	UnLock(ctx context.Context, lockId, ownerId string) error
}
