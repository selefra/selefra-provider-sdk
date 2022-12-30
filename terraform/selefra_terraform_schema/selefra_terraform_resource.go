package selefra_terraform_schema

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/terraform/bridge"
	"go.uber.org/zap"
)

// ------------------------------------------------- TerraformBridgeGetter ---------------------------------------------

// TerraformBridgeGetter This interface call returns an rpc bridge to the provider created and initialized with the terraform
type TerraformBridgeGetter func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask) *bridge.TerraformBridge

// ------------------------------------------------- ListIdsFunc -------------------------------------------------------

// ListIdsFunc Gets one or more list functions
type ListIdsFunc func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) ([]string, *schema.Diagnostics)

// ToSelefraDataSource Convert the ListFunc to a Selefra DataSource so that you can connect it to the Selefra Provider
func (x ListIdsFunc) ToSelefraDataSource(getter TerraformBridgeGetter, resourceName string) schema.DataSource {
	return schema.DataSource{
		Pull: func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {
			diagnostics := schema.NewDiagnostics()
			ids, d := x(ctx, clientMeta, taskClient, task, resultChannel)
			if diagnostics.AddDiagnostics(d).HasError() {
				return diagnostics
			}
			if len(ids) == 0 {
				clientMeta.DebugF("exec table list return zero ids", zap.String("taskId", task.TaskId), zap.String("tableName", task.Table.TableName))
				return nil
			}
			clientMeta.DebugF("exec table list return no zero ids", zap.String("taskId", task.TaskId), zap.String("tableName", task.Table.TableName), zap.Strings("ids", ids))
			return getter(ctx, clientMeta, taskClient, task).ListByIds(ctx, resourceName, ids, clientMeta, taskClient, task, resultChannel)
		},
	}
}

// ------------------------------------------------- --------------------------------------------------------------------

// SelefraTerraformResource Indicates that the resource is associated with to terraform
type SelefraTerraformResource struct {

	// Table's name
	SelefraTableName string

	// The terraform resource corresponding to this table
	TerraformResourceName string

	// You can provide some description information, which will be included in the automatic document generation
	Description string

	// A table can have child tables whose data depends on the current table
	//SubTables []*schema.Table
	SubTables []string

	// The table must return a list of ids
	ListIdsFunc ListIdsFunc
}

func (x *SelefraTerraformResource) ToTable(getter TerraformBridgeGetter) (*schema.Table, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	if x.ListIdsFunc == nil {
		return nil, diagnostics.AddErrorMsg("table %s's ListIdsFunc must be implemented", x.SelefraTableName)
	}

	return &schema.Table{
		TableName:   x.SelefraTableName,
		Description: x.Description,
		DataSource:  x.ListIdsFunc.ToSelefraDataSource(getter, x.TerraformResourceName),
	}, diagnostics
}
