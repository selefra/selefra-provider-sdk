package column_value_extractor

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestColumnValueExtractorStructSelector(t *testing.T) {

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

	extractResult, diagnostics := StructSelector("Foo.FooValue").Extract(context.Background(), nil, nil, nil, nil, nil, result)

	assert.True(t, diagnostics == nil || !diagnostics.HasError())
	assert.Equal(t, extractResult, "foo")
}
