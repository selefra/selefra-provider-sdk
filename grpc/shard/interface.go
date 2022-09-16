package shard

import (
	"context"
	"encoding/json"
	"fmt"

	plugin "github.com/hashicorp/go-plugin"
	"github.com/selefra/selefra-utils/pkg/json_util"

	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
	"github.com/selefra/selefra-provider-sdk/storage_factory"
)

var (
	PluginMap = map[string]plugin.Plugin{
		"provider": &Plugin{},
	}
	VersionPluginMap = map[int]plugin.PluginSet{
		V1: PluginMap,
	}
)

type ProviderClient interface {
	Provider
	PullTables(ctx context.Context, in *PullTablesRequest) (ProviderServerStream, error)
}

type ProviderServer interface {
	Provider
	PullTables(context.Context, *PullTablesRequest, ProviderServerSender) error
}

type Provider interface {
	Init(ctx context.Context, in *ProviderInitRequest) (*ProviderInitResponse, error)
	GetProviderInformation(ctx context.Context, in *GetProviderInformationRequest) (*GetProviderInformationResponse, error)
	GetProviderConfig(ctx context.Context, in *GetProviderConfigRequest) (*GetProviderConfigResponse, error)
	SetProviderConfig(ctx context.Context, in *SetProviderConfigRequest) (*SetProviderConfigResponse, error)
	DropTableAll(ctx context.Context, in *ProviderDropTableAllRequest) (*ProviderDropTableAllResponse, error)
	CreateAllTables(ctx context.Context, in *ProviderCreateAllTablesRequest) (*ProviderCreateAllTablesResponse, error)
}

// -------------------------------------------------------------------------------------------------------------------------

type ProviderInitRequest struct {
	Storage        *Storage
	Workspace      *string
	ProviderConfig *string
	IsInstallInit  *bool
}

type ProviderInitResponse struct {
	Diagnostics *schema.Diagnostics `json:"diagnostics"`
}

// -------------------------------------------------------------------------------------------------------------------------

type ProviderDropTableAllRequest struct{}

type ProviderCreateAllTablesRequest struct{}

type ProviderDropTableAllResponse struct {
	Diagnostics *schema.Diagnostics `json:"diagnostics"`
}

type ProviderCreateAllTablesResponse struct {
	Diagnostics *schema.Diagnostics `json:"diagnostics"`
}

// -------------------------------------------------------------------------------------------------------------------------

type ProviderServerStream interface {
	Recv() (*PullTablesResponse, error)
}

type ProviderServerSender interface {
	Send(*PullTablesResponse) error
}

// -------------------------------------------------------------------------------------------------------------------------

// FakeProviderServerSender The Provider simulates RPC calls when doing integration tests
type FakeProviderServerSender struct {
	ProviderServerSender
}

func NewFakeProviderServerSender() *FakeProviderServerSender {
	return &FakeProviderServerSender{}
}

func (f *FakeProviderServerSender) Send(response *PullTablesResponse) error {
	if response.Diagnostics != nil {
		fmt.Println("sender: " + response.Diagnostics.ToString())
	} else {
		fmt.Println("sender: " + json_util.ToJsonString(response))
	}
	return nil
}

// ------------------------------------------------- GetProviderInformation --------------------------------------------

type GetProviderInformationRequest struct{}

type GetProviderInformationResponse struct {
	Name    string                   `json:"name"`
	Version string                   `json:"version"`
	Tables  map[string]*schema.Table `json:"tables"`

	DefaultConfigTemplate string `json:"default_config_template"`

	Diagnostics *schema.Diagnostics
}

// ------------------------------------------------- GetProviderConfig -------------------------------------------------

type GetProviderConfigRequest struct{}

type GetProviderConfigResponse struct {
	Name        string              `json:"name"`
	Version     string              `json:"version"`
	Config      string              `json:"config"`
	Diagnostics *schema.Diagnostics `json:"diagnostics"`
}

// ------------------------------------------------- SetProviderConfig -------------------------------------------------

type SetProviderConfigRequest struct {
	Storage        *Storage `json:"storage"`
	ProviderConfig *string  `json:"provider_config"`
}

type SetProviderConfigResponse struct {
	Diagnostics *schema.Diagnostics `json:"diagnostics"`
}

// ProviderInitResponse

type PullTablesRequest struct {

	// The table to be pulled, this later expands to support parameters
	Tables []string `json:"tables"`

	// Maximum number of threads used
	MaxGoroutines uint64 `json:"max_goroutines"`

	// Pull timeout period
	Timeout int64 `json:"timeout"`
}

// NewPullAllTablesRequest The Provider integration test simulates the RPC environment
func NewPullAllTablesRequest() *PullTablesRequest {
	return &PullTablesRequest{
		Tables:        []string{"*"},
		MaxGoroutines: 1,
		Timeout:       1000 * 60 * 60,
	}
}

type PullTablesResponse struct {
	FinishedTables map[string]bool     `json:"finished_tables"`
	TableCount     uint64              `json:"table_count"`
	Table          string              `json:"table"`
	Diagnostics    *schema.Diagnostics `json:"diagnostic"`
}

//
//type Table struct {
//	Name        string       `json:"name"`
//	Description string       `json:"description"`
//	Columns     []Column     `json:"columns"`
//	Constraints []Constraint `json:"constraints"`
//}
//
//type Column struct {
//	Name        string     `json:"name"`
//	Description string     `json:"description"`
//	Type        ColumnType `json:"type"`
//	Meta        ColumnMeta `json:"meta"`
//}
//
//type ColumnMeta struct {
//	Resolver     ResolverMeta `json:"resolver"`
//	IgnoreExists bool         `json:"ignore_exists"`
//}
//
//type ResolverMeta struct {
//	Name    string `json:"name"`
//	Builtin bool   `json:"builtin"`
//}
//
//type Constraint struct {
//	Type      ConstraintType `json:"type"`
//	Columns   []string       `json:"columns"`
//	TableName string         `json:"table_name"`
//}
//
//type ColumnType int
//
//const (
//	INVALID ColumnType = iota
//	BOOL
//	SMALLINT
//	INT
//	BIGINT
//	FLOAT
//	UUID
//	STRING
//	BYTE_ARRAY
//	STRING_ARRAY
//	INT_ARRAY
//	TIMESTAMP
//	JSON
//	UUID_ARRAY
//	INET
//	INET_ARRAY
//	CIDR
//	CIDR_ARRAY
//	MAC_ADDR
//	MAC_ADDR_ARRAY
//)
//
//type ConstraintType int
//
//const (
//	PRIMARY_KEY ConstraintType = iota
//
//	FOREIGN_KEY
//)

// ProviderConfiguration Provider General configuration
type ProviderConfiguration struct {
	Storage Storage `json:"storage"`

	NamespaceSuffix string `json:"namespace_suffix"`
}

type Storage struct {
	Type           StorageType `json:"type"`
	StorageOptions []byte      `json:"options"`
}

func (x *Storage) GetStorageType() storage_factory.StorageType {
	switch x.Type {
	case POSTGRESQL:
		return storage_factory.StorageTypePostgresql
	default:
		panic("storage type not supported")
	}
}

func (x *Storage) GetStorageOptions() storage.CreateStorageOptions {
	switch x.Type {
	case POSTGRESQL:
		options := &postgresql_storage.PostgresqlStorageOptions{}
		err := json.Unmarshal(x.StorageOptions, options)
		if err != nil {
			return nil
		}
		return options
	default:
		panic("storage type not supported")
	}
}

type StorageType int

const (
	POSTGRESQL StorageType = iota
	MYSQL
)
