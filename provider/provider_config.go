package provider

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/spf13/viper"
)

type ConfigMeta struct {

	// If the Provider has a configuration file, you can use this method to return a default configuration file template that will be written to
	// You can write the value constant directly or get it from the web, but it must be a valid YAML file or it will be rejected
	GetDefaultConfigTemplate func(ctx context.Context) string

	// You can provide a function to verify the correctness of the configuration file.
	// It is highly recommended that you configure this function when working with configuration files
	Validation func(ctx context.Context, config *viper.Viper) *schema.Diagnostics

	runtime *ConfigMetaRuntime
}

func (x *ConfigMeta) Runtime() *ConfigMetaRuntime {
	return x.runtime
}
