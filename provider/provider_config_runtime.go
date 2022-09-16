package provider

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/spf13/viper"
	"strings"
)

// ConfigMetaRuntime After the provider runs, the relevant configuration may be passed from the previous dynamic pass
type ConfigMetaRuntime struct {
	myConfig *ConfigMeta

	// provider's configuration
	providerConfigYamlString string

	// parsed provider's configuration
	providerConfigViper *viper.Viper
}

// is this provider need configuration?
func (x *ConfigMetaRuntime) isNeedConfig() bool {
	if x.myConfig == nil {
		return false
	}
	return x.myConfig.GetDefaultConfigTemplate != nil && x.myConfig.Validation != nil
}

func NewConfigRuntime(ctx context.Context, myConfig *ConfigMeta, configYamlString *string) (runtime *ConfigMetaRuntime, diagnostics *schema.Diagnostics) {

	diagnostics = schema.NewDiagnostics()
	runtime = &ConfigMetaRuntime{
		// bind this context
		myConfig: myConfig,
	}

	// a viper is initialized by default regardless of whether a configuration file is passed, because some provider developers don't have a habit of So before you use a pointer,
	// let's say it's nil, so they have to be compatible
	providerConfigViper := viper.New()
	providerConfigViper.SetConfigType("yaml")
	runtime.providerConfigViper = providerConfigViper

	// if provider do not need configuration, then just return, do not need init config
	if !runtime.isNeedConfig() {
		return
	}

	// update config, It can be initialized without passing
	if configYamlString != nil {
		diagnostics.AddDiagnostics(runtime.UpdateProviderConfig(ctx, configYamlString))
	}

	return
}

func (x *ConfigMetaRuntime) UpdateProviderConfig(ctx context.Context, configYamlString *string) (diagnostics *schema.Diagnostics) {
	diagnostics = schema.NewDiagnostics()

	// configuration must not empty
	if configYamlString == nil {
		diagnostics.AddErrorMsg("config init error: config is missing")
		return
	}

	// parser config file
	configViper := viper.New()
	configViper.SetConfigType("yaml")
	err := configViper.ReadConfig(strings.NewReader(*configYamlString))
	if err != nil {
		diagnostics.AddErrorMsg("config init error: parser yaml error, %s", err.Error())

		return
	}
	x.providerConfigYamlString = *configYamlString
	x.providerConfigViper = configViper

	// validation config file
	if x.myConfig.Validation != nil && diagnostics.AddDiagnostics(x.myConfig.Validation(ctx, x.providerConfigViper)).HasError() {
		return
	}

	return
}
