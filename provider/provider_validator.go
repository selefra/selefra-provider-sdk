package provider

import (
	"context"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"gopkg.in/yaml.v3"
)

// Used to verify the validity of the provider

type providerValidator struct {
	myProvider *Provider
}

// To improve the efficiency of developing providers, check whether the declarations of providers are valid
func (x *providerValidator) validate(ctx context.Context, myProvider *Provider, clientMeta *schema.ClientMeta) *schema.Diagnostics {

	x.myProvider = myProvider

	diagnostics := schema.NewDiagnostics()

	// name
	providerNameLimit := 60
	if myProvider.Name == "" {
		diagnostics.AddErrorMsg(x.buildErrorMsg("provider name must not be empty"))
	} else if len(myProvider.Name) > providerNameLimit {
		diagnostics.AddErrorMsg(x.buildErrorMsg("provider name length cannot greater than %d", providerNameLimit))
	}

	// version
	if myProvider.Version == "" {
		diagnostics.AddErrorMsg(x.buildErrorMsg("version must not be empty"))
	}

	// provider config
	if myProvider.ConfigMeta.GetDefaultConfigTemplate != nil {
		configTemplate := myProvider.ConfigMeta.GetDefaultConfigTemplate(ctx)
		var node yaml.Node
		err := yaml.Unmarshal([]byte(configTemplate), &node)
		if err != nil {
			return diagnostics.AddErrorMsg(x.buildErrorMsg("GetDefaultConfigTemplate return default config template parse .yaml error: %s", err.Error()))
		}
	}

	// table
	if len(myProvider.runtime.tableMap) == 0 {
		// allow empty provider for test
		//diagnostics.AddWarn("provider %s table map is empty", myProvider.Name)
	} else {
		// Each table is self-tested
		for _, table := range myProvider.runtime.tableMap {
			// This is non-blocking, so that you try to detect all the errors at once, rather than squeezing them one by one
			diagnostics.AddDiagnostics(table.Runtime().Validate(ctx, clientMeta, nil, table))
		}
	}

	return diagnostics
}

func (x *providerValidator) buildErrorMsg(msg string, args ...any) string {
	return fmt.Sprintf("provider %s is invalid: %s", x.myProvider.Name, fmt.Sprintf(msg, args...))
}
