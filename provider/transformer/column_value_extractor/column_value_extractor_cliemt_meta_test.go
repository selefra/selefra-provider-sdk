package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorClientMeta(t *testing.T) {

	// build client meta for test
	clientMeta := &schema.ClientMeta{}
	runtime, _ := schema.NewClientMetaRuntime(context.Background(), "./", "test", clientMeta, nil, true)
	_ = reflect_util.SetStructPtrUnExportedStrField(clientMeta, "runtime", runtime)
	clientMeta.SetItem("foo", "bar")

	// not default
	v, d := ClientMetaGetItem("foo").Extract(context.Background(), clientMeta, nil, nil, nil, nil, nil)
	assert.True(t, d == nil || !d.HasError())
	assert.Equal(t, v, "bar")

	v, d = ClientMetaGetItem("foo2").Extract(context.Background(), clientMeta, nil, nil, nil, nil, nil)
	assert.True(t, d == nil || !d.HasError())
	assert.Nil(t, v)

	// use default
	v, d = ClientMetaGetItemOrDefault("foo3", "bar3").Extract(context.Background(), clientMeta, nil, nil, nil, nil, nil)
	assert.Equal(t, v, "bar3")

}
