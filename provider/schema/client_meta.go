package schema

import (
	"context"
	"github.com/spf13/viper"
)

type ClientMeta struct {
	ClientLogger

	// If your application calls the API using a client, you can initialize it here, and when you call the API,
	// you pass the client you initialized here, so you don't have to re-initialize the client every time you call the API
	InitClient func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) ([]any, *Diagnostics)

	// You can use a custom Logger, otherwise a default Logger will be initialized
	InitLogger func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) (ClientLogger, *Diagnostics)

	runtime *ClientMetaRuntime
}

var _ ClientLogger = &ClientMeta{}

func (x *ClientMeta) Runtime() *ClientMetaRuntime {
	return x.runtime
}

func (x *ClientMeta) SetItem(itemName string, itemValue any) {
	x.runtime.SetItem(itemName, itemValue)
}

func (x *ClientMeta) GetItem(itemName string) any {
	return x.runtime.GetItem(itemName)
}

func (x *ClientMeta) GetStringItem(itemName, defaultValue string) string {
	return x.runtime.GetStringItem(itemName, defaultValue)
}

func (x *ClientMeta) GetIntItem(itemName string, defaultValue int) int {
	return x.runtime.GetIntItem(itemName, defaultValue)
}

func (x *ClientMeta) ClearItem() {
	x.runtime.ClearItem()
}

func (x *ClientMeta) GetClientSlice() []any {
	return x.runtime.client
}
