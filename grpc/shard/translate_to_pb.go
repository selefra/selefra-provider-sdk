package shard

import (
	"github.com/selefra/selefra-provider-sdk/grpc/internal"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// ------------------------------------------------- Init --------------------------------------------------------------

func ToPbProviderInitRequest(request *ProviderInitRequest) *internal.ProviderInit_Request {
	if request == nil {
		return nil
	}
	return &internal.ProviderInit_Request{
		Storage:        ToPbStorage(request.Storage),
		Workspace:      request.Workspace,
		ProviderConfig: request.ProviderConfig,
		IsInstallInit:  request.IsInstallInit,
	}
}

func ToPbGetProviderInitResponse(response *ProviderInitResponse) *internal.ProviderInit_Response {
	if response == nil {
		return nil
	}
	return &internal.ProviderInit_Response{
		Diagnostics: ToPbDiagnostics(response.Diagnostics),
	}
}

// ------------------------------------------------- GetProviderInformation --------------------------------------------

func ToPbGetProviderInformationRequest(_ *GetProviderInformationRequest) *internal.GetProviderInformation_Request {
	return &internal.GetProviderInformation_Request{}
}

func ToPbGetProviderInformationResponse(in *GetProviderInformationResponse) *internal.GetProviderInformation_Response {
	if in == nil {
		return nil
	}
	return &internal.GetProviderInformation_Response{
		Name:                  in.Name,
		Version:               in.Version,
		Tables:                ToPbSchemaTablesByMap(in.Tables),
		DefaultConfigTemplate: in.DefaultConfigTemplate,
		Diagnostics:           ToPbDiagnostics(in.Diagnostics),
	}
}

func ToPbSchemaTablesByMap(tables map[string]*schema.Table) map[string]*internal.Table {
	if tables == nil || len(tables) == 0 {
		return nil
	}
	result := make(map[string]*internal.Table, len(tables))
	for k, v := range tables {
		result[k] = ToPbSchemaTable(v)
	}
	return result
}

func ToPbSchemaTablesByList(tables []*schema.Table) []*internal.Table {
	if tables == nil || len(tables) == 0 {
		return nil
	}
	result := make([]*internal.Table, len(tables))
	for index, v := range tables {
		result[index] = ToPbSchemaTable(v)
	}
	return result
}

func ToPbSchemaTable(table *schema.Table) *internal.Table {
	if table == nil {
		return nil
	}
	return &internal.Table{
		Namespace:   table.GetNamespace(),
		TableName:   table.TableName,
		Description: table.Description,
		Columns:     ToPbSchemaColumns(table.Columns),
		SubTables:   ToPbSchemaTablesByList(table.SubTables),
		Version:     table.Version,
	}
}

func ToPbSchemaColumns(columns []*schema.Column) []*internal.Column {
	if columns == nil || len(columns) == 0 {
		return nil
	}
	result := make([]*internal.Column, len(columns))
	for index, column := range columns {
		result[index] = ToPbSchemaColumn(column)
	}
	return result
}

func ToPbSchemaColumn(column *schema.Column) *internal.Column {
	if column == nil {
		return nil
	}
	return &internal.Column{
		Name:        column.ColumnName,
		Type:        internal.ColumnType(column.Type),
		Description: column.Description,
	}
}

// ------------------------------------------------- ProviderConfig ----------------------------------------------------

func ToPbGetProviderConfigRequest(_ *GetProviderConfigRequest) *internal.GetProviderConfig_Request {
	return &internal.GetProviderConfig_Request{}
}

func ToPbGetProviderConfigResponse(in *GetProviderConfigResponse) *internal.GetProviderConfig_Response {
	if in == nil {
		return nil
	}
	return &internal.GetProviderConfig_Response{
		Name:        in.Name,
		Version:     in.Version,
		Config:      in.Config,
		Diagnostics: ToPbDiagnostics(in.Diagnostics),
	}
}

func ToPbSetProviderConfigRequest(in *SetProviderConfigRequest) *internal.SetProviderConfig_Request {
	if in == nil {
		return nil
	}
	return &internal.SetProviderConfig_Request{
		Storage:        ToPbStorage(in.Storage),
		ProviderConfig: in.ProviderConfig,
	}
}

func ToPbSetProviderConfigResponse(in *SetProviderConfigResponse) *internal.SetProviderConfig_Response {
	if in == nil || in.Diagnostics == nil {
		return nil
	}

	return &internal.SetProviderConfig_Response{
		Diagnostics: ToPbDiagnostics(in.Diagnostics),
	}
}

func ToPbStorage(storage *Storage) *internal.Storage {
	if storage == nil {
		return nil
	}
	storageOptionsJsonString, _ := storage.GetStorageOptions().ToJsonString()
	return &internal.Storage{
		Type:           internal.StorageType(storage.GetStorageType()),
		StorageOptions: storageOptionsJsonString,
	}
}

// ------------------------------------------------- PullTable ---------------------------------------------------------

func ToPbPullTablesRequest(in *PullTablesRequest) *internal.PullTables_Request {
	if in == nil {
		return nil
	}
	return &internal.PullTables_Request{
		Tables:        in.Tables,
		MaxGoroutines: in.MaxGoroutines,
		Timeout:       in.Timeout,
	}
}

func ToPbPullTablesResponse(in *PullTablesResponse) *internal.PullTables_Response {
	if in == nil {
		return nil
	}

	return &internal.PullTables_Response{
		FinishedTables: in.FinishedTables,
		TableCount:     in.TableCount,
		Table:          in.Table,
		Diagnostics:    ToPbDiagnostics(in.Diagnostics),
	}
}

func ToPbDiagnostics(diagnostics *schema.Diagnostics) []*internal.Diagnostic {
	if diagnostics == nil || diagnostics.Size() == 0 {
		return nil
	}
	result := make([]*internal.Diagnostic, diagnostics.Size())
	for index, diagnostic := range diagnostics.GetDiagnosticSlice() {
		result[index] = &internal.Diagnostic{
			DiagnosticLevel: internal.Diagnostic_DiagnosticLevel(diagnostic.Level()),
			Content:         diagnostic.Content(),
		}
	}
	return result
}

// ------------------------------------------------- PullTable ---------------------------------------------------------

func ToPbDropTableRequest(in *ProviderDropTableAllRequest) *internal.DropTableAll_Request {
	return &internal.DropTableAll_Request{}
}

func ToPbCreateTableRequest(in *ProviderCreateAllTablesRequest) *internal.CreateAllTables_Request {
	return &internal.CreateAllTables_Request{}
}
