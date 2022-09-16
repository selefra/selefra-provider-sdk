package shard

import (
	"github.com/selefra/selefra-provider-sdk/grpc/internal"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// ------------------------------------------------- Init --------------------------------------------------------------

func ToShardProviderInitRequest(request *internal.ProviderInit_Request) *ProviderInitRequest {
	return &ProviderInitRequest{
		Storage:        ToShardStorage(request.Storage),
		Workspace:      request.Workspace,
		ProviderConfig: request.ProviderConfig,
		IsInstallInit:  request.IsInstallInit,
	}
}

func ToShardProviderInitResponse(response *internal.ProviderInit_Response) *ProviderInitResponse {
	if response == nil {
		return nil
	}
	return &ProviderInitResponse{
		Diagnostics: ToShardDiagnostics(response.Diagnostics),
	}
}

// ------------------------------------------------- GetProviderInformation --------------------------------------------

func ToShardGetProviderInformationRequest(_ *internal.GetProviderInformation_Request) *GetProviderInformationRequest {
	return &GetProviderInformationRequest{}
}

func ToShardGetProviderInformationResponse(in *internal.GetProviderInformation_Response) *GetProviderInformationResponse {
	if in == nil {
		return nil
	}
	return &GetProviderInformationResponse{
		Name:                  in.GetName(),
		Version:               in.GetVersion(),
		Tables:                ToSchemaTablesByMap(in.GetTables()),
		DefaultConfigTemplate: in.DefaultConfigTemplate,
	}
}

func ToSchemaTablesByMap(tables map[string]*internal.Table) map[string]*schema.Table {
	if tables == nil || len(tables) == 0 {
		return nil
	}
	result := make(map[string]*schema.Table, len(tables))
	for k, v := range tables {
		result[k] = ToSchemaTable(v)
	}
	return result
}

func ToSchemaTablesByList(tables []*internal.Table) []*schema.Table {
	if tables == nil || len(tables) == 0 {
		return nil
	}
	result := make([]*schema.Table, len(tables))
	for index, v := range tables {
		result[index] = ToSchemaTable(v)
	}
	return result
}

func ToSchemaTable(table *internal.Table) *schema.Table {
	if table == nil {
		return nil
	}

	schemaTable := &schema.Table{
		TableName:   table.GetTableName(),
		Description: table.GetDescription(),
		Columns:     ToSchemaColumns(table.GetColumns()),
		SubTables:   ToSchemaTablesByList(table.GetSubTables()),
		Version:     table.GetVersion(),
	}
	schemaTable.Runtime().Namespace = table.GetNamespace()
	return schemaTable
}

func ToSchemaColumns(columns []*internal.Column) []*schema.Column {
	if columns == nil || len(columns) == 0 {
		return nil
	}
	result := make([]*schema.Column, len(columns))
	for index, column := range columns {
		result[index] = ToSchemaColumn(column)
	}
	return result
}

func ToSchemaColumn(column *internal.Column) *schema.Column {
	if column == nil {
		return nil
	}
	return &schema.Column{
		ColumnName:  column.GetName(),
		Type:        schema.ColumnType(column.GetType()),
		Description: column.GetDescription(),
	}
}

// ------------------------------------------------- GetProviderConfig -------------------------------------------------

func ToShardGetProviderConfigRequest(_ *internal.GetProviderConfig_Request) *GetProviderConfigRequest {
	return &GetProviderConfigRequest{}
}

func ToShardGetProviderConfigResponse(in *internal.GetProviderConfig_Response) *GetProviderConfigResponse {
	if in == nil {
		return nil
	}
	return &GetProviderConfigResponse{
		Name:        in.GetName(),
		Version:     in.GetVersion(),
		Config:      in.Config,
		Diagnostics: ToShardDiagnostics(in.Diagnostics),
	}
}

// ------------------------------------------------- SetProviderConfig -------------------------------------------------

func ToShardSetProviderConfigurationRequest(in *internal.SetProviderConfig_Request) *SetProviderConfigRequest {
	if in == nil {
		return nil
	}
	return &SetProviderConfigRequest{
		Storage:        ToShardStorage(in.Storage),
		ProviderConfig: in.ProviderConfig,
	}
}

func ToShardStorage(storage *internal.Storage) *Storage {
	if storage == nil {
		return nil
	}
	return &Storage{
		Type:           StorageType(storage.GetType()),
		StorageOptions: []byte(storage.GetStorageOptions()),
	}
}

func ToShardPullTablesRequest(in *internal.PullTables_Request) *PullTablesRequest {
	if in == nil {
		return nil
	}
	return &PullTablesRequest{
		Tables:        in.GetTables(),
		MaxGoroutines: in.GetMaxGoroutines(),
		Timeout:       in.GetTimeout(),
	}
}

func ToShardSetProviderConfigResponse(in *internal.SetProviderConfig_Response) *SetProviderConfigResponse {
	if in == nil {
		return nil
	}
	return &SetProviderConfigResponse{
		Diagnostics: ToShardDiagnostics(in.GetDiagnostics()),
	}
}

// ------------------------------------------------- PullTables --------------------------------------------------------

func ToShardPullTablesResponse(in *internal.PullTables_Response) *PullTablesResponse {
	if in == nil {
		return nil
	}
	return &PullTablesResponse{
		FinishedTables: in.GetFinishedTables(),
		TableCount:     in.GetTableCount(),
		Table:          in.Table,
		Diagnostics:    ToShardDiagnostics(in.Diagnostics),
	}
}

func ToShardDiagnostics(pbDiagnosticSlice []*internal.Diagnostic) *schema.Diagnostics {
	result := schema.NewDiagnostics()
	if pbDiagnosticSlice == nil || len(pbDiagnosticSlice) == 0 {
		return result
	}
	for _, diagnostic := range pbDiagnosticSlice {
		result.Add(schema.NewDiagnostic(schema.DiagnosticLevel(diagnostic.GetDiagnosticLevel()), diagnostic.GetContent()))
	}
	return result
}

// ---------------------------------------------------------------------------------------------------------------------

func ToShardProviderDropResponse(response *internal.DropTableAll_Response) *ProviderDropTableAllResponse {
	if response == nil {
		return nil
	}
	return &ProviderDropTableAllResponse{
		Diagnostics: ToShardDiagnostics(response.Diagnostics),
	}
}

func ToShardProviderCreateResponse(response *internal.CreateAllTables_Response) *ProviderCreateAllTablesResponse {
	if response == nil {
		return nil
	}
	return &ProviderCreateAllTablesResponse{
		Diagnostics: ToShardDiagnostics(response.Diagnostics),
	}
}
