package serve

import (
	"github.com/hashicorp/go-plugin"
	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"google.golang.org/grpc"
)

var (
	HandSharkConfig = plugin.HandshakeConfig{
		MagicCookieKey:   "MINIQUERY_PLUGIN_COOKIE_KEY",
		MagicCookieValue: "647dd4cc-46c7-4886-b751-8020f0e151d7",
	}
)

func Serve(name string, provider shard.ProviderServer) {

	if name == "" {
		panic("name is empty")
	}

	if provider == nil {
		panic("provider is nil")
	}
	serve(name, provider)
}

func serve(name string, provider shard.ProviderServer) {
	// LOGGER
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: HandSharkConfig,
		VersionedPlugins: map[int]plugin.PluginSet{
			shard.V1: map[string]plugin.Plugin{
				"provider": &shard.Plugin{Impl: provider},
			},
		},
		GRPCServer: func(opts []grpc.ServerOption) *grpc.Server {
			return grpc.NewServer(opts...)
		},
	})
}
