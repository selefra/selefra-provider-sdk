package bridge

import (
	"context"
	shim "github.com/pulumi/pulumi-terraform-bridge/v3/pkg/tfshim"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

type TerraformBridge struct {
	providerExecPath string
	provider         shim.Provider
}

func NewTerraformBridge(providerExecPath string) *TerraformBridge {
	return &TerraformBridge{
		providerExecPath: providerExecPath,
	}
}

func (x *TerraformBridge) StartBridge(ctx context.Context, providerConfig map[string]any) error {

	// run
	provider, err := StartProvider(ctx, x.providerExecPath, "5")
	if err != nil {
		return err
	}

	// config
	err = provider.Configure(provider.NewResourceConfig(providerConfig))
	if err != nil {
		return err
	}

	x.provider = provider
	return nil
}

type ListIdsFunc func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, resultChannel chan<- any) ([]string, *schema.Diagnostics)

func (x *TerraformBridge) ListByIds(ctx context.Context, resourceName string, ids []string, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {
	diagnostics := schema.NewDiagnostics()
	for _, id := range ids {
		diagnostics.AddDiagnostics(x.GetDetail(ctx, resourceName, id, map[string]any{}, map[string]any{}, clientMeta, client, task, resultChannel))
	}
	return diagnostics
}

func (x *TerraformBridge) GetDetail(ctx context.Context, resourceName string, id string, objectMap, meta map[string]any, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {
	resource := x.provider.ResourcesMap().Get(resourceName)
	state, err := resource.InstanceState(id, objectMap, meta)
	if err != nil {
		return schema.NewDiagnostics().AddError(err)
	}
	newState, err := x.provider.Refresh(resourceName, state)
	if err != nil {
		return schema.NewDiagnostics().AddError(err)
	}
	object, err := newState.Object(resource.Schema())
	if err != nil {
		return schema.NewDiagnostics().AddError(err)
	}
	resultChannel <- object
	return nil
}

func (x *TerraformBridge) GetProvider() shim.Provider {
	return x.provider
}

func (x *TerraformBridge) Shutdown() error {
	return x.provider.Stop()
}
