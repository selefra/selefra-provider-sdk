package provider

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/selefra/selefra-utils/pkg/id_util"
	"github.com/selefra/selefra-utils/pkg/string_util"

	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer"
	"github.com/selefra/selefra-provider-sdk/storage"
	"github.com/selefra/selefra-provider-sdk/storage_factory"
)

// ProviderRuntime provider's runtime, most of the runtime logic is encapsulated here
type ProviderRuntime struct {

	// The provider to which this runtime is bound
	myProvider *Provider

	workspace string

	// For all tables in the provider <TableName, schema.Table>
	tableMap map[string]*schema.Table

	// It is used to verify the correctness of the Provider
	validator providerValidator

	// This provider is the Storage currently in use
	storage storage.Storage

	// The converter currently used by this provider
	transformer *transformer.Transformer
}

func NewProviderRuntime(ctx context.Context, myProvider *Provider) (runtime *ProviderRuntime, diagnostics *schema.Diagnostics) {

	diagnostics = schema.NewDiagnostics()
	runtime = &ProviderRuntime{
		// bind this context
		myProvider: myProvider,
	}

	return
}

func (x *ProviderRuntime) validate(ctx context.Context, clientMeta *schema.ClientMeta) *schema.Diagnostics {
	return x.validator.validate(ctx, x.myProvider, clientMeta)
}

func (x *ProviderRuntime) initTablesRuntime(ctx context.Context, provider *Provider) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	x.tableMap = make(map[string]*schema.Table)

	// TODO 2022-7-21 19:32:36 If it is slow, change it to concurrent execution
	for _, table := range provider.TableList {

		// The table name is not allow repeated
		if _, exists := x.tableMap[table.TableName]; exists {
			diagnostics.AddErrorMsg("table runtime error, table name %s is not uniq", table.TableName)
		}

		x.tableMap[table.TableName] = table
		diagnostics.AddDiagnostics(table.Runtime().Init(ctx, &x.myProvider.ClientMeta, nil, table))
	}

	return diagnostics
}

// ------------------------------------------------- workspace ---------------------------------------------------------

func (x *ProviderRuntime) initWorkspace(ctx context.Context, workspace *string) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if workspace == nil || *workspace == "" {
		return diagnostics.AddErrorMsg("init provider runtime error: workspace can not empty")
	}

	x.workspace = *workspace

	return nil
}

// ------------------------------------------------- Storage -----------------------------------------------------------

func (x *ProviderRuntime) initStorage(ctx context.Context, storage *shard.Storage, clientMeta *schema.ClientMeta) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	if storage == nil {
		return diagnostics.AddErrorMsg("storage init error: storage is nil")
	}
	providerStorage, d := storage_factory.NewStorage(ctx, storage.GetStorageType(), storage.GetStorageOptions())
	if diagnostics.AddDiagnostics(d).HasError() {
		return diagnostics
	}
	x.storage = providerStorage
	x.storage.SetClientMeta(clientMeta)
	return diagnostics
}

// ------------------------------------------------- Provider table management related ---------------------------------

var ErrorStorageNotInit = errors.New("storage not init")

func (x *ProviderRuntime) CreateAllTables(ctx context.Context) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if x.storage == nil {
		return diagnostics.AddErrorMsg(ErrorStorageNotInit.Error())
	}

	tables := make([]*schema.Table, 0)
	for _, table := range x.tableMap {
		tables = append(tables, table)
	}
	return x.storage.TablesCreate(ctx, tables)
}

// DropAllTables Delete all tables of the Provider
func (x *ProviderRuntime) DropAllTables(ctx context.Context) *schema.Diagnostics {

	diagnostics := schema.NewDiagnostics()

	if x.storage == nil {
		return diagnostics.AddErrorMsg("storage not init")
	}

	tables := make([]*schema.Table, 0)
	for _, table := range x.tableMap {
		tables = append(tables, table)
	}
	return x.storage.TablesDrop(ctx, tables)
}

// ------------------------------------------------- -------------------------------------------------------------------

const errorMessageStorageNotInit = "storage not initialized"

// PullTables Pull the given resource
func (x *ProviderRuntime) PullTables(ctx context.Context, request *shard.PullTablesRequest, sender shard.ProviderServerSender) error {

	diagnostics := schema.NewDiagnostics()

	// Data sources must be initialized before resources can be pulled
	if x.storage == nil {
		diagnostics.AddErrorMsg(errorMessageStorageNotInit)
		x.myProvider.ClientMeta.DebugF("pull table exit, occur error: %s", diagnostics.ToString())
		return sender.Send(x.buildPullTablesResponseWithDiagnostics(diagnostics))
	}

	// Calculate and verify the table to be pulled
	pullTables, d := x.computeNeedPullRootTables(request.Tables)
	if diagnostics.AddDiagnostics(d).HasError() {
		x.myProvider.ClientMeta.DebugF("pull table exit, occur error: %s", diagnostics.ToString())
		return sender.Send(x.buildPullTablesResponseWithDiagnostics(diagnostics))
	}

	// Create a data source task actuator
	dataSourceExecutor, d := schema.NewDataSourcePullExecutor(request.MaxGoroutines, &x.myProvider.ClientMeta, &x.myProvider.ErrorsHandlerMeta)
	if diagnostics.AddDiagnostics(d).HasError() {
		x.myProvider.ClientMeta.DebugF("pull table exit, occur error: %s", diagnostics.ToString())
		return sender.Send(x.buildPullTablesResponseWithDiagnostics(diagnostics))
	}

	totalTableCount := x.computeAllNeedPullTablesCount(pullTables...)
	finishTable := make(map[string]bool, 0)
	finishTableLock := sync.RWMutex{}

	// Collect some information from the fetching process and so on
	diagnosticsChannel := make(chan *schema.Diagnostics, 1000)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for diagnostics := range diagnosticsChannel {
			// If it is empty, there is no need to send it forward
			if diagnostics == nil || diagnostics.IsEmpty() {
				continue
			}
			finishTableLock.RLock()
			_ = sender.Send(&shard.PullTablesResponse{
				FinishedTables: finishTable,
				TableCount:     totalTableCount,
				Table:          "",
				Diagnostics:    diagnostics,
			})
			finishTableLock.RUnlock()
		}
	}()

	// The tables to be pulled are then submitted in turn
	for _, table := range pullTables {

		task := &schema.DataSourcePullTask{
			TaskId:             id_util.RandomId(),
			Ctx:                context.Background(),
			Table:              table,
			DiagnosticsChannel: diagnosticsChannel,
			ResultHandler:      x.resultHandler,
			TaskDoneCallback: func(ctx context.Context, clientMeta *schema.ClientMeta, task *schema.DataSourcePullTask) *schema.Diagnostics {
				table := task.Table
				finishTableLock.Lock()
				defer finishTableLock.Unlock()

				finishTable[table.TableName] = true

				err := sender.Send(&shard.PullTablesResponse{
					FinishedTables: finishTable,
					TableCount:     totalTableCount,
					Table:          table.TableName,
					Diagnostics:    nil,
				})

				if err != nil {
					clientMeta.ErrorF("taskId = %s, send rpc error: %s", task.TaskId, err)
					return schema.NewDiagnostics().AddErrorMsg("table %s task done, send rpc error: %s", task.Table.TableName, err.Error())
				}

				clientMeta.DebugF("taskId = %s, table = %s, send finished rpc done.", table.TableName, task.TaskId)

				return nil
			},
			IsRootTask:   true,
			IsExpandDone: false,
			Client:       nil,
		}
		diagnostics.AddDiagnostics(dataSourceExecutor.Submit(context.Background(), task))
		// taskId --> tableName relation, after just use taskId
		x.myProvider.ClientMeta.DebugF("taskId = %s, commit task to executor, table name = %s", task.TaskId, task.Table.TableName)
	}
	x.myProvider.ClientMeta.DebugF("all task submit to executor done, shutdown and wait...")

	diagnostics.AddDiagnostics(dataSourceExecutor.ShutdownAndAwaitTermination(context.Background()))
	diagnosticsChannel <- diagnostics

	close(diagnosticsChannel)
	wg.Wait()

	x.myProvider.ClientMeta.DebugF("pull table queue done, exit function")

	return nil
}

func (x *ProviderRuntime) resultHandler(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, result any) (*schema.Rows, []any, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	resultSlice := make([]any, 0)
	if x.myProvider.TransformerMeta.DataSourcePullResultAutoExpand {
		// expand if is array or slice
		reflectValue := reflect.ValueOf(result)
		switch reflectValue.Kind() {
		case reflect.Array, reflect.Slice:
			for index := 0; index < reflectValue.Len(); index++ {
				item := reflectValue.Index(index).Interface()
				resultSlice = append(resultSlice, item)
			}
		default:
			resultSlice = append(resultSlice, result)
		}
	} else {
		// no expand
		resultSlice = append(resultSlice, result)
	}

	var saveSuccessRows *schema.Rows
	saveSuccessResultSlice := make([]any, 0)
	for _, result := range resultSlice {

		// step 1. parser from raw result to row
		row, d := x.handleSingleResult(ctx, clientMeta, client, task, result)
		diagnostics.Add(d)
		if d != nil && d.HasError() {
			// If an error occurs and ignore is configured, the end occurs
			if x.myProvider.ErrorsHandlerMeta.IsIgnore(schema.IgnoredErrorOnSaveResult) {
				clientMeta.ErrorF("taskId = %s, IgnoredErrorOnSaveResult")
				continue
			} else {
				return nil, nil, diagnostics
			}
		}

		// step 2. save row to database
		d = x.storage.Insert(ctx, task.Table, row.ToRows())
		diagnostics.AddDiagnostics(d)

		if d != nil && d.HasError() {

			// log transaction row error
			msg := string_util.NewStringBuilder()
			msg.WriteString(fmt.Sprintf("taskId = %s, table %s rows save to storage error: %s, raw result: %s, row: %s", task.TaskId, task.Table.TableName, d.ToString(), result, row.String()))
			if task.ParentRow != nil {
				msg.WriteString(fmt.Sprintf("\n parent row = %s", task.ParentRow.String()))
			}
			if task.ParentRawResult != nil {
				msg.WriteString(fmt.Sprintf("parent raw result: %s", task.ParentRawResult))
			}
			clientMeta.Error(msg.String())

			// If an error occurs and ignore is configured, the end occurs
			if x.myProvider.ErrorsHandlerMeta.IsIgnore(schema.IgnoredErrorOnSaveResult) {
				clientMeta.ErrorF("taskId = %s, IgnoredErrorOnSaveResult")
				continue
			} else {
				return nil, nil, diagnostics
			}
		} else {
			// merge rows
			isRowsMergeSuccess := true
			if saveSuccessRows == nil {
				saveSuccessRows = row.ToRows()
			} else {
				err := saveSuccessRows.AppendRow(row)
				if err != nil {
					clientMeta.ErrorF("taskId = %s, IgnoredErrorOnSaveResult")
					isRowsMergeSuccess = false
				}
			}
			// merge result slice, only rows merge success, then merge raw result
			if isRowsMergeSuccess {
				saveSuccessResultSlice = append(saveSuccessResultSlice, result)
			}
		}
	}

	return saveSuccessRows, saveSuccessResultSlice, diagnostics
}

func (x *ProviderRuntime) handleSingleResult(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, result any) (*schema.Row, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	// Analytical results
	row, d := x.transformer.TransformResult(ctx, client, task, result)
	hasError := d != nil && d.HasError()

	if hasError {

		// log transaction row error
		msg := string_util.NewStringBuilder()
		msg.WriteString(fmt.Sprintf("taskId = %s, table %s row transformer error: %s, raw result: %s", task.TaskId, task.Table.TableName, d.ToString(), result))
		if task.ParentRow != nil {
			msg.WriteString(fmt.Sprintf("\n parent row = %s", task.ParentRow.String()))
		}
		if task.ParentRawResult != nil {
			msg.WriteString(fmt.Sprintf("parent raw result: %s", task.ParentRawResult))
		}
		clientMeta.Error(msg.String())

		// If an error occurs and ignore is configured, the end occurs
		if x.myProvider.ErrorsHandlerMeta.IsIgnore(schema.IgnoredErrorOnTransformerRow) {
			clientMeta.DebugF("taskId = %s, IgnoredErrorOnTransformerRow", task.TaskId)
			return nil, diagnostics
		}

	}

	// If an error occurs and ignore is configured, the end occurs
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}

	return row, diagnostics
}

func (x *ProviderRuntime) buildPullTablesResponseWithDiagnostics(diagnostics *schema.Diagnostics) *shard.PullTablesResponse {
	return &shard.PullTablesResponse{
		FinishedTables: nil,
		TableCount:     0,
		Table:          "",
		Diagnostics:    diagnostics,
	}
}

// AllTableNameWildcard Table name Wildcard, which matches all tables
const AllTableNameWildcard = "*"

// Calculate which tables to pull, and do some checking and expansion
func (x *ProviderRuntime) computeNeedPullRootTables(tableNames []string) ([]*schema.Table, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	// for distinct
	pullTableNameSet := make(map[string]struct{}, 0)
	// What are the tables to pull
	pullTables := make([]*schema.Table, 0)

	var isAllTableNameWildcard bool

	for _, tableName := range tableNames {
		if tableName != AllTableNameWildcard {
			continue
		}
		isAllTableNameWildcard = true
		// If it is a wildcard, expand it
		for tableName, table := range x.tableMap {
			// distinct
			if _, exists := pullTableNameSet[tableName]; exists {
				continue
			}
			pullTables = append(pullTables, table)
			pullTableNameSet[tableName] = struct{}{}
		}
		break
	}
	if !isAllTableNameWildcard {
		// nowTable: rootTable
		tableRootMap := make(map[string]string, 0)
		for rootTableName, rootTable := range x.tableMap {
			for _, tableName := range x.flatTable(rootTable) {
				tableRootMap[tableName] = rootTableName
			}
		}

		for _, tableName := range tableNames {
			// If it is not a wildcard character, it is a common table name
			rootTableName, exists := tableRootMap[tableName]
			if !exists {
				diagnostics.AddErrorMsg("pull provider %s's table failed, because table %s not exists", x.myProvider.Name, tableName)
				continue
			}

			// distinct
			if _, exists := pullTableNameSet[rootTableName]; exists {
				continue
			}
			pullTables = append(pullTables, x.tableMap[rootTableName])
			pullTableNameSet[rootTableName] = struct{}{}

		}
	}

	return pullTables, diagnostics
}

func (x *ProviderRuntime) computeAllNeedPullTablesCount(tables ...*schema.Table) uint64 {
	count := uint64(0)
	for _, table := range tables {
		count += 1 + x.computeAllNeedPullTablesCount(table.SubTables...)
	}
	return count
}

func (x *ProviderRuntime) flatTable(table *schema.Table) []string {
	if table == nil {
		return nil
	}
	tableNameSlice := []string{table.TableName}
	for _, subTables := range table.SubTables {
		tableNameSlice = append(tableNameSlice, x.flatTable(subTables)...)
	}
	return tableNameSlice
}
