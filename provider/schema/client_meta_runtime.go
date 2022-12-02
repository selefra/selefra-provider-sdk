package schema

import (
	"context"
	"fmt"
	"github.com/selefra/selefra-utils/pkg/runtime_util"
	"github.com/spf13/viper"
	"strings"
	"sync"
)

type ClientMetaRuntime struct {

	// this runtime bind this context
	myClientMeta *ClientMeta

	client []any

	itemMap     map[string]any
	itemMapLock sync.RWMutex

	Workspace string
}

func NewClientMetaRuntime(ctx context.Context, workspace, providerName string, myClientMeta *ClientMeta, providerConfigViper *viper.Viper, isRunUserMetaInit bool) (runtime *ClientMetaRuntime, diagnostics *Diagnostics) {
	diagnostics = NewDiagnostics()
	runtime = &ClientMetaRuntime{
		myClientMeta: myClientMeta,
	}
	myClientMeta.runtime = runtime

	// bind context can not be nil
	if runtime.myClientMeta == nil {
		diagnostics.AddErrorMsg("ClientMetaRuntime bind ClientMeta must not nil")
		return
	}

	runtime.itemMap = make(map[string]any)
	runtime.itemMapLock = sync.RWMutex{}

	// logger
	if runtime.myClientMeta.InitLogger != nil && isRunUserMetaInit {
		if diagnostics.Add(runtime.execInitLogger(ctx, providerConfigViper)).HasError() {
			return nil, diagnostics
		}
	} else {
		// Logger has a default value that allows users to log
		clientLogger, err := NewDefaultClientLogger(NewDefaultClientLoggerConfig(workspace, providerName))
		if err != nil {
			return nil, diagnostics.AddErrorMsg("init default client logger error: %s", err.Error())
		}
		runtime.myClientMeta.ClientLogger = clientLogger
	}

	// client, Client has no default value, if the user sets it, it's passed in, and if it's not set, it's nil
	if runtime.myClientMeta.InitClient != nil && isRunUserMetaInit {
		if diagnostics.Add(runtime.execInitClient(ctx, providerConfigViper)).HasError() {
			return nil, diagnostics
		}
	}

	// save workspace
	runtime.Workspace = workspace

	return
}

// for try catch panic
func (x *ClientMetaRuntime) execInitClient(ctx context.Context, config *viper.Viper) (diagnostics *Diagnostics) {

	diagnostics = NewDiagnostics()

	defer func() {
		if r := recover(); r != nil {
			msg := strings.Builder{}
			msg.WriteString(fmt.Sprintf("client runtime exec InitClient panic: %#v", r))
			diagnostics.AddErrorMsg(msg.String())

			msg.WriteString("\nStack: \n")
			msg.WriteString(runtime_util.Stack())
			x.myClientMeta.Error(msg.String())
		}
	}()

	client, d := x.myClientMeta.InitClient(ctx, x.myClientMeta, config)
	if !diagnostics.AddDiagnostics(d).HasError() {
		x.client = client
	}
	return diagnostics
}

func (x *ClientMetaRuntime) execInitLogger(ctx context.Context, config *viper.Viper) (diagnostics *Diagnostics) {

	diagnostics = NewDiagnostics()

	defer func() {
		if r := recover(); r != nil {
			msg := strings.Builder{}
			msg.WriteString(fmt.Sprintf("client runtime exec InitLogger panic: %#v", r))
			diagnostics.AddErrorMsg(msg.String())

			// log do not initialized
			//msg.WriteString("\nStack: \n")
			//msg.WriteString(runtime_util.Stack())
			//x.myClientMeta.Error(msg.String())
		}
	}()

	logger, d := x.myClientMeta.InitLogger(ctx, x.myClientMeta, config)
	if !diagnostics.AddDiagnostics(d).HasError() {
		x.myClientMeta.ClientLogger = logger
	}
	return diagnostics
}

func (x *ClientMetaRuntime) SetItem(itemName string, itemValue any) {
	x.itemMapLock.Lock()
	defer x.itemMapLock.Unlock()
	x.itemMap[itemName] = itemValue
}

func (x *ClientMetaRuntime) GetItem(itemName string) any {
	x.itemMapLock.RLock()
	defer x.itemMapLock.RUnlock()
	return x.itemMap[itemName]
}

func (x *ClientMetaRuntime) GetStringItem(itemName, defaultValue string) string {
	item := x.GetItem(itemName)
	if item == nil {
		return defaultValue
	}
	value, ok := item.(string)
	if !ok {
		return defaultValue
	}
	return value
}

func (x *ClientMetaRuntime) GetIntItem(itemName string, defaultValue int) int {
	item := x.GetItem(itemName)
	if item == nil {
		return defaultValue
	}
	value, ok := item.(int)
	if !ok {
		return defaultValue
	}
	return value
}

func (x *ClientMetaRuntime) ClearItem() {

	x.itemMapLock.RLock()
	defer x.itemMapLock.RUnlock()

	x.itemMap = make(map[string]any)
}
