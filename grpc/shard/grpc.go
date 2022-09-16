package shard

import (
	"context"

	plugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/selefra/selefra-provider-sdk/grpc/internal"
)

const (
	V1 = 1

	Unmanaged = -1
)

type GRPCClient struct {
	broker *plugin.GRPCBroker
	client internal.ProviderClient
}

func (g *GRPCClient) GetProviderInformation(ctx context.Context, in *GetProviderInformationRequest) (*GetProviderInformationResponse, error) {
	res, err := g.client.GetProviderInformation(ctx, ToPbGetProviderInformationRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardGetProviderInformationResponse(res), nil
}

func (g *GRPCClient) GetProviderConfig(ctx context.Context, in *GetProviderConfigRequest) (*GetProviderConfigResponse, error) {
	res, err := g.client.GetProviderConfig(ctx, ToPbGetProviderConfigRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardGetProviderConfigResponse(res), nil
}

func (g *GRPCClient) SetProviderConfig(ctx context.Context, in *SetProviderConfigRequest) (*SetProviderConfigResponse, error) {
	res, err := g.client.SetProviderConfig(ctx, ToPbSetProviderConfigRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardSetProviderConfigResponse(res), nil
}

func (g *GRPCClient) Init(ctx context.Context, in *ProviderInitRequest) (*ProviderInitResponse, error) {
	res, err := g.client.Init(ctx, ToPbProviderInitRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardProviderInitResponse(res), nil
}

func (g *GRPCClient) PullTables(ctx context.Context, in *PullTablesRequest) (ProviderServerStream, error) {
	res, err := g.client.PullTables(ctx, ToPbPullTablesRequest(in))
	if err != nil {
		return nil, err
	}
	return &Recv{in: res}, nil
}

func (g *GRPCClient) DropTableAll(ctx context.Context, in *ProviderDropTableAllRequest) (*ProviderDropTableAllResponse, error) {
	res, err := g.client.DropTableAll(ctx, ToPbDropTableRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardProviderDropResponse(res), nil
}

func (g *GRPCClient) CreateAllTables(ctx context.Context, in *ProviderCreateAllTablesRequest) (*ProviderCreateAllTablesResponse, error) {
	res, err := g.client.CreateAllTables(ctx, ToPbCreateTableRequest(in))
	if err != nil {
		return nil, err
	}
	return ToShardProviderCreateResponse(res), nil
}

type Recv struct {
	in internal.Provider_PullTablesClient
}

func (t *Recv) Recv() (*PullTablesResponse, error) {
	v, err := t.in.Recv()
	if err != nil {
		return nil, err
	}
	return ToShardPullTablesResponse(v), nil
}

type Send struct {
	in internal.Provider_PullTablesServer
}

func (s *Send) Send(p *PullTablesResponse) error {
	return s.in.Send(ToPbPullTablesResponse(p))
}

type GRPCServer struct {
	// This is the real implementation
	Impl ProviderServer
	internal.UnimplementedProviderServer
}

func (g *GRPCServer) Init(ctx context.Context, in *internal.ProviderInit_Request) (*internal.ProviderInit_Response, error) {
	v, err := g.Impl.Init(ctx, ToShardProviderInitRequest(in))
	if err != nil {
		return nil, err
	}
	return ToPbGetProviderInitResponse(v), nil
}

func (g *GRPCServer) GetProviderInformation(ctx context.Context, in *internal.GetProviderInformation_Request) (*internal.GetProviderInformation_Response, error) {
	v, err := g.Impl.GetProviderInformation(ctx, ToShardGetProviderInformationRequest(in))
	if err != nil {
		return nil, err
	}
	return ToPbGetProviderInformationResponse(v), nil
}

func (g *GRPCServer) GetProviderConfig(ctx context.Context, in *internal.GetProviderConfig_Request) (*internal.GetProviderConfig_Response, error) {
	v, err := g.Impl.GetProviderConfig(ctx, ToShardGetProviderConfigRequest(in))
	if err != nil {
		return nil, err
	}
	return ToPbGetProviderConfigResponse(v), nil
}

func (g *GRPCServer) SetProviderConfig(ctx context.Context, in *internal.SetProviderConfig_Request) (*internal.SetProviderConfig_Response, error) {
	v, err := g.Impl.SetProviderConfig(ctx, ToShardSetProviderConfigurationRequest(in))
	if err != nil {
		return nil, err
	}
	return ToPbSetProviderConfigResponse(v), nil
}

func (g *GRPCServer) PullTables(req *internal.PullTables_Request, send internal.Provider_PullTablesServer) error {
	return g.Impl.PullTables(context.Background(), ToShardPullTablesRequest(req), &Send{in: send})
}

func (g *GRPCServer) DropTableAll(context.Context, *internal.DropTableAll_Request) (*internal.DropTableAll_Response, error) {
	v, err := g.Impl.DropTableAll(context.Background(), &ProviderDropTableAllRequest{})
	if err != nil {
		return nil, err
	}
	return &internal.DropTableAll_Response{Diagnostics: ToPbDiagnostics(v.Diagnostics)}, nil
}

func (g *GRPCServer) CreateAllTables(context.Context, *internal.CreateAllTables_Request) (*internal.CreateAllTables_Response, error) {
	v, err := g.Impl.CreateAllTables(context.Background(), &ProviderCreateAllTablesRequest{})
	if err != nil {
		return nil, err
	}
	return &internal.CreateAllTables_Response{Diagnostics: ToPbDiagnostics(v.Diagnostics)}, nil
}

// Plugin This is the implementation of plugin.GRPCServer so we can serve/consume this.
type Plugin struct {
	// GRPCPlugin must still implement the Stub interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl ProviderServer
}

func (p *Plugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	internal.RegisterProviderServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

func (p *Plugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return &GRPCClient{broker: broker, client: internal.NewProviderClient(c)}, nil
}
