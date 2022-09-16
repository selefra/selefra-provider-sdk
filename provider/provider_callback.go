package provider

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// EventCallback Provider has some event callback methods, You can register callback events to do the right thing at the right time
type EventCallback struct {

	// Callback method that is triggered after the installation is complete init
	AfterInstallInitEventCallback func(ctx context.Context, provider *Provider) *schema.Diagnostics

	// The callback method that is executed every time the Provider is started
	InitEventCallback func(ctx context.Context, provider *Provider) *schema.Diagnostics
}
