package schema

import (
	"context"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientMeta_GetIntItem_execInitClient(t *testing.T) {
	diagnostics := NewDiagnostics()

	// normal
	meta := &ClientMeta{
		InitClient: func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) ([]any, *Diagnostics) {
			return nil, nil
		},
	}
	runtime, d := NewClientMetaRuntime(context.Background(), "./", "test", "v0.0.1", meta, nil, true)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.NotNil(t, runtime)

	// panic
	meta = &ClientMeta{
		InitClient: func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) ([]any, *Diagnostics) {
			panic("something wrong")
		},
	}
	runtime, d = NewClientMetaRuntime(context.Background(), "./", "test", "v0.0.1",meta, nil, true)
	assert.True(t, diagnostics.Add(d).HasError())
	assert.Nil(t, runtime)
}

func TestClientMeta_GetIntItem_execInitLogger(t *testing.T) {
	diagnostics := NewDiagnostics()

	// normal
	meta := &ClientMeta{
		InitLogger: func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) (ClientLogger, *Diagnostics) {
			return nil, nil
		},
	}
	runtime, d := NewClientMetaRuntime(context.Background(), "./", "test", "v0.0.1",meta, nil, true)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.NotNil(t, runtime)

	// panic
	meta = &ClientMeta{
		InitLogger: func(ctx context.Context, clientMeta *ClientMeta, config *viper.Viper) (ClientLogger, *Diagnostics) {
			panic("something wrong, wrong...")
		},
	}
	runtime, d = NewClientMetaRuntime(context.Background(), "./", "test", "v0.0.1",meta, nil, true)
	assert.True(t, diagnostics.Add(d).HasError())
	assert.Nil(t, runtime)
}
