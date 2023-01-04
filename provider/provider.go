package provider

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_convertor"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"go.uber.org/zap"

	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer"
)

// Provider To represent a source from which data can be retrieved, the Provider's implementation is to declare and configure this struct
type Provider struct {
	shard.ProviderServer

	// provider's name
	Name string

	// provider's version
	Version string

	// provider's description, let others know what this provider does in general
	Description string

	// A provider may have many tables. Here are the corresponding tables
	TableList []*schema.Table

	ConfigMeta ConfigMeta

	EventCallback EventCallback

	ClientMeta schema.ClientMeta

	TransformerMeta schema.TransformerMeta

	ErrorsHandlerMeta schema.ErrorsHandlerMeta

	runtime *ProviderRuntime
}

var _ shard.ProviderServer = &Provider{}

const (
	ErrMsgNotInitRuntime = "provider runtime not init"
)

// GetProviderInformation Obtain the information about the Provider, must after Init
func (x *Provider) GetProviderInformation(ctx context.Context, in *shard.GetProviderInformationRequest) (response *shard.GetProviderInformationResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.GetProviderInformationResponse{
				Name:        x.Name,
				Version:     x.Version,
				Tables:      nil,
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider GetProviderInformation panic: %s", r),
			}
		}
	}()

	// runtime must already init
	if x.runtime == nil {
		return &shard.GetProviderInformationResponse{
			Diagnostics: schema.NewDiagnostics().AddErrorMsg(ErrMsgNotInitRuntime),
		}, nil
	}

	defaultConfigTemplate := ""
	if x.ConfigMeta.GetDefaultConfigTemplate != nil {
		defaultConfigTemplate = x.ConfigMeta.GetDefaultConfigTemplate(ctx)
	}

	return &shard.GetProviderInformationResponse{
		Name:                  x.Name,
		Version:               x.Version,
		Tables:                x.runtime.tableMap,
		DefaultConfigTemplate: defaultConfigTemplate,
	}, nil
}

// GetProviderConfig Obtain the configuration information of the Provider, must after Init
func (x *Provider) GetProviderConfig(ctx context.Context, in *shard.GetProviderConfigRequest) (response *shard.GetProviderConfigResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.GetProviderConfigResponse{
				Name:        x.Name,
				Version:     x.Version,
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider GetProviderConfig panic: %s", r),
			}
		}
	}()

	// runtime must already init
	if x.runtime == nil {
		return &shard.GetProviderConfigResponse{
			Diagnostics: schema.NewDiagnostics().AddErrorMsg(ErrMsgNotInitRuntime),
		}, nil
	}

	return &shard.GetProviderConfigResponse{
		Name:    x.Name,
		Version: x.Version,
		Config:  x.ConfigMeta.runtime.providerConfigYamlString,
	}, nil
}

// SetProviderConfig Set the configuration information of the Provider, must after Init
func (x *Provider) SetProviderConfig(ctx context.Context, request *shard.SetProviderConfigRequest) (response *shard.SetProviderConfigResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.SetProviderConfigResponse{
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider SetProviderConfig panic: %s", r),
			}
		}
	}()

	diagnostics := schema.NewDiagnostics()

	// runtime must already init
	if x.runtime == nil {
		return &shard.SetProviderConfigResponse{
			Diagnostics: diagnostics.AddErrorMsg(ErrMsgNotInitRuntime),
		}, nil
	}

	if x.ConfigMeta.runtime == nil {
		return &shard.SetProviderConfigResponse{
			Diagnostics: diagnostics.AddErrorMsg(ErrMsgNotInitRuntime),
		}, nil
	}
	// 1. update config runtime
	x.ConfigMeta.runtime.UpdateProviderConfig(ctx, request.ProviderConfig)

	// 2. init ClientMeta, must after init config
	clientMetaRuntime, d := schema.NewClientMetaRuntime(ctx, x.runtime.workspace, x.Name, x.Version, &x.ClientMeta, x.ConfigMeta.runtime.providerConfigViper, true)
	if diagnostics.AddDiagnostics(d).HasError() {
		return &shard.SetProviderConfigResponse{
			Diagnostics: diagnostics,
		}, nil
	}
	_ = reflect_util.SetStructPtrUnExportedStrField(&x.ClientMeta, "runtime", clientMetaRuntime)

	// 3. update Storage, must after init client meta
	if request.Storage != nil {
		if diagnostics.AddDiagnostics(x.runtime.initStorage(ctx, request.Storage, &x.ClientMeta)).HasError() {
			return &shard.SetProviderConfigResponse{
				Diagnostics: diagnostics,
			}, nil
		}
	}

	return &shard.SetProviderConfigResponse{
		Diagnostics: diagnostics,
	}, nil
}

// -------------------------------------------------------------------------------------------------------------------------

// PullTables Pull the given resource
func (x *Provider) PullTables(ctx context.Context, request *shard.PullTablesRequest, sender shard.ProviderServerSender) (err error) {

	defer func() {
		if r := recover(); r != nil {
			x.ClientMeta.ErrorF("exec PullTables panic", zap.Error(r.(error)))
			err = sender.Send(&shard.PullTablesResponse{
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec PullTables panic: %s", r),
			})
		}
	}()

	// runtime must already init
	if x.runtime == nil {
		return sender.Send(&shard.PullTablesResponse{
			Diagnostics: schema.NewDiagnostics().AddErrorMsg(ErrMsgNotInitRuntime),
		})
	}

	return x.runtime.PullTables(ctx, request, sender)
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *Provider) DropTableAll(ctx context.Context, request *shard.ProviderDropTableAllRequest) (response *shard.ProviderDropTableAllResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.ProviderDropTableAllResponse{
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider DropTableAll panic: %s", r),
			}
		}
	}()

	return &shard.ProviderDropTableAllResponse{Diagnostics: x.runtime.DropAllTables(ctx)}, nil
}

func (x *Provider) CreateAllTables(ctx context.Context, request *shard.ProviderCreateAllTablesRequest) (response *shard.ProviderCreateAllTablesResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.ProviderCreateAllTablesResponse{
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider CreateAllTables panic: %s", r),
			}
		}
	}()

	return &shard.ProviderCreateAllTablesResponse{Diagnostics: x.runtime.CreateAllTables(ctx)}, nil
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *Provider) Init(ctx context.Context, request *shard.ProviderInitRequest) (response *shard.ProviderInitResponse, err error) {

	defer func() {
		if r := recover(); r != nil {
			response = &shard.ProviderInitResponse{
				Diagnostics: schema.NewDiagnostics().AddErrorMsg("exec provider Init panic: %s", r),
			}
		}
	}()

	diagnostics := schema.NewDiagnostics()

	// request must not nil
	if request == nil {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics.AddErrorMsg("can not init, because request is nil"),
		}, nil
	}

	// new runtime
	runtime, d := NewProviderRuntime(ctx, x)
	if diagnostics.AddDiagnostics(d).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}
	x.runtime = runtime

	// init workspace
	if diagnostics.AddDiagnostics(x.runtime.initWorkspace(ctx, request.Workspace)).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}

	// init config and it's runtime
	configRuntime, d := NewConfigRuntime(ctx, &x.ConfigMeta, request.ProviderConfig)
	if diagnostics.AddDiagnostics(d).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}
	x.ConfigMeta.runtime = configRuntime

	// init client meta, must after config init done, if on init give provider configuration, then init user's meta function, or else just init myself log
	clientMetaRuntime, d := schema.NewClientMetaRuntime(ctx, x.runtime.workspace, x.Name, x.Version, &x.ClientMeta, configRuntime.providerConfigViper, len(configRuntime.providerConfigViper.AllKeys()) != 0)
	if diagnostics.AddDiagnostics(d).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}
	_ = reflect_util.SetStructPtrUnExportedStrField(&x.ClientMeta, "runtime", clientMetaRuntime)
	x.ClientMeta.DebugF("init client meta success, workspace = %s", x.runtime.workspace)

	// init error handler meta runtime
	errorHandlerMetaRuntime := schema.NewErrorsHandlerMetaRuntime(&x.ErrorsHandlerMeta)
	_ = reflect_util.SetStructPtrUnExportedStrField(&x.ErrorsHandlerMeta, "runtime", errorHandlerMetaRuntime)
	x.ClientMeta.Debug("init error handler runtime success")

	// init runtime storage, must after client meta init done
	if diagnostics.AddDiagnostics(runtime.initStorage(ctx, request.Storage, &x.ClientMeta)).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}

	// init table runtime
	if diagnostics.AddDiagnostics(x.runtime.initTablesRuntime(ctx, x)).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}

	// ColumnValueConvertor priority:
	// 1. provider's custom ColumnValueConvertor
	// 2. if configuration DefaultColumnValueConvertor, use it, or else ignored it
	// 3. use storage's ColumnValueConvertor, if it config it, or else ignored it
	// 4. finally, use DefaultColumnValueConvertor
	var columnValueConvertor schema.ColumnValueConvertor
	if x.TransformerMeta.ColumnValueConvertor != nil {
		columnValueConvertor = x.TransformerMeta.ColumnValueConvertor
	} else {
		if x.TransformerMeta.IsUseDefaultColumnValueConvertor() {
			columnValueConvertor = column_value_convertor.NewDefaultTypeConvertor(&x.ClientMeta, x.TransformerMeta.DefaultColumnValueConvertorBlackList)
		} else {
			columnValueConvertor = x.runtime.storage.NewColumnValueConvertor()
		}
		if columnValueConvertor == nil {
			columnValueConvertor = column_value_convertor.NewDefaultTypeConvertor(&x.ClientMeta, x.TransformerMeta.DefaultColumnValueConvertorBlackList)
		}
	}
	// create transformer
	x.runtime.transformer = transformer.NewTransformer(&x.ClientMeta, columnValueConvertor, &x.ErrorsHandlerMeta)

	// provider check it self, must after table init done
	if diagnostics.AddDiagnostics(x.runtime.validate(ctx, &x.ClientMeta)).HasError() {
		return &shard.ProviderInitResponse{
			Diagnostics: diagnostics,
		}, nil
	}

	// at last, call the after install event callback
	if request.IsInstallInit != nil && *request.IsInstallInit {

		// create table
		if diagnostics.AddDiagnostics(x.runtime.CreateAllTables(ctx)).HasError() {
			return &shard.ProviderInitResponse{
				Diagnostics: diagnostics,
			}, nil
		}

		// is after install init
		if x.EventCallback.AfterInstallInitEventCallback != nil {
			// TODO
			if diagnostics.AddDiagnostics(x.EventCallback.AfterInstallInitEventCallback(ctx, x)).HasError() {
				return &shard.ProviderInitResponse{
					Diagnostics: diagnostics,
				}, nil
			}
		}

	} else {

		// if not install init
		if x.EventCallback.InitEventCallback != nil {
			// TODO
			if diagnostics.AddDiagnostics(x.EventCallback.InitEventCallback(ctx, x)).HasError() {
				return &shard.ProviderInitResponse{
					Diagnostics: diagnostics,
				}, nil
			}
		}
	}

	return &shard.ProviderInitResponse{
		Diagnostics: diagnostics,
	}, nil
}

// ----------------------------------------------------------------------------------------------------------------------

func (x *Provider) Runtime() *ProviderRuntime {
	return x.runtime
}
