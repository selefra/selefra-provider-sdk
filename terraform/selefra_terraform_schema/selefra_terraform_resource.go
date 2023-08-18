package selefra_terraform_schema

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/terraform/bridge"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
	"time"
)

// ------------------------------------------------- TerraformBridgeGetter ---------------------------------------------

// TerraformBridgeGetter This interface call returns an rpc bridge to the provider created and initialized with the terraform
type TerraformBridgeGetter func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask) *bridge.TerraformBridge

// ------------------------------------------------- ListIdsFunc -------------------------------------------------------

//// ListIdsFunc Gets one or more list functions
//type ListIdsFunc func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) ([]string, *schema.Diagnostics)
//
//// ToSelefraDataSource Convert the ListFunc to a Selefra DataSource so that you can connect it to the Selefra Provider
//func (x ListIdsFunc) ToSelefraDataSource(getter TerraformBridgeGetter, resourceName string) schema.DataSource {
//	return schema.DataSource{
//		Pull: func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {
//			diagnostics := schema.NewDiagnostics()
//			ids, d := x(ctx, clientMeta, taskClient, task, resultChannel)
//			if diagnostics.AddDiagnostics(d).HasError() {
//				return diagnostics
//			}
//			if len(ids) == 0 {
//				clientMeta.DebugF("exec table list return zero ids", zap.String("taskId", task.TaskId), zap.String("tableName", task.Table.TableName))
//				return nil
//			}
//			clientMeta.DebugF("exec table list return no zero ids", zap.String("taskId", task.TaskId), zap.String("tableName", task.Table.TableName), zap.Strings("ids", ids))
//			return getter(ctx, clientMeta, taskClient, task).ListByIds(ctx, resourceName, ids, clientMeta, taskClient, task, resultChannel)
//		},
//	}
//}

// ------------------------------------------------- --------------------------------------------------------------------

// ResourceRequestParam Request parameters for the resource
type ResourceRequestParam struct {

	// ID of resource
	ID string

	// The parameters required to request the resource
	ArgumentMap map[string]any
}

func NewResourceRequestParam() *ResourceRequestParam {
	return &ResourceRequestParam{
		ID:          "",
		ArgumentMap: make(map[string]any),
	}
}

func NewResourceRequestParamWithID(id string) *ResourceRequestParam {
	param := NewResourceRequestParam()
	param.ID = id
	return param
}

func NewResourceRequestParamWithArgumentMap(argumentMap map[string]any) *ResourceRequestParam {
	param := NewResourceRequestParam()
	param.ArgumentMap = argumentMap
	return param
}

func NewResourceRequestParamWithIDAndArgumentMap(id string, argumentMap map[string]any) *ResourceRequestParam {
	param := NewResourceRequestParam()
	param.ID = id
	param.ArgumentMap = argumentMap
	return param
}

// ListResourceParamsFunc Returns several parameters that request Resource usage
type ListResourceParamsFunc func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) ([]*ResourceRequestParam, *schema.Diagnostics)

func GetMemoryUsage() int {
	u, err := mem.VirtualMemory()
	if err != nil {
		return 0
	}
	return int(u.Used / 1024 / 1024)
}

// ToSelefraDataSource Convert the ListFunc to a Selefra DataSource so that you can connect it to the Selefra Provider
func (x ListResourceParamsFunc) ToSelefraDataSource(getter TerraformBridgeGetter, resourceName string) schema.DataSource {
	return schema.DataSource{
		Pull: func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {
			for {
				if GetMemoryUsage() > 1024 {
					time.Sleep(1 * time.Second)
				} else {
					break
				}
			}

			diagnostics := schema.NewDiagnostics()

			resourceRequestParams, d := x(ctx, clientMeta, taskClient, task, resultChannel)
			if diagnostics.AddDiagnostics(d).HasError() {
				return diagnostics
			}

			if len(resourceRequestParams) == 0 {
				clientMeta.DebugF("exec table list return zero resource request params", zap.String("taskId", task.TaskId), zap.String("tableName", task.Table.TableName))
				return nil
			}

			terraformBridge := getter(ctx, clientMeta, taskClient, task)
			for _, resourceRequestParam := range resourceRequestParams {

				if resourceRequestParam.ArgumentMap == nil {
					resourceRequestParam.ArgumentMap = make(map[string]any, 0)
				}

				id, exists := resourceRequestParam.ArgumentMap["id"]
				if !exists {
					// map not exists
					if resourceRequestParam.ID != "" {
						resourceRequestParam.ArgumentMap["id"] = resourceRequestParam.ID
					}
				} else {
					// map exists
					if resourceRequestParam.ID == "" {
						idString, ok := id.(string)
						if ok {
							resourceRequestParam.ID = idString
						}
					}
				}
				diagnostics.AddDiagnostics(terraformBridge.GetDetail(ctx, resourceName, resourceRequestParam.ID, resourceRequestParam.ArgumentMap, map[string]any{}, clientMeta, taskClient, task, resultChannel))
			}
			return diagnostics
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

	// If your table needs to extend the default call method, you can implement this function
	// The default is to use Task once per client
	// But you can call the client multiple times by making multiple copies of the client
	// When a value is returned, it is called once with each of the returned clients
	// The second parameter, task, if it is not returned, uses the original task. If it is returned, it must be the same length as the client and correspond one to one
	ExpandClientTask func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask) []*schema.ClientTaskContext

	// A table can have child tables whose data depends on the current table
	//SubTables []*schema.Table
	SubTables []string

	// take resource params
	ListResourceParamsFunc ListResourceParamsFunc
}

func (x *SelefraTerraformResource) ToTable(getter TerraformBridgeGetter) (*schema.Table, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	if x.ListResourceParamsFunc == nil {
		return nil, diagnostics.AddErrorMsg("table %s's ListIdsFunc must be implemented", x.SelefraTableName)
	}

	return &schema.Table{
		TableName:        x.SelefraTableName,
		Description:      x.Description,
		ExpandClientTask: x.ExpandClientTask,
		DataSource:       x.ListResourceParamsFunc.ToSelefraDataSource(getter, x.TerraformResourceName),
	}, diagnostics
}
