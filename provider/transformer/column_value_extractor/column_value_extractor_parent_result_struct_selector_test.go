package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorParentResultStructSelector(t *testing.T) {
	type Foo struct {
		FooValue string
	}

	type Bar struct {
		Foo *Foo
	}

	result := &Bar{
		Foo: &Foo{
			FooValue: "foo",
		},
	}

	task := &schema.DataSourcePullTask{
		ParentRawResult: result,
	}
	extractResult, diagnostics := ParentResultStructSelector("Foo.FooValue").Extract(context.Background(), nil, nil, task, nil, nil, nil)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, extractResult, "foo")
}
