package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestColumnValueExtractorStructSelectorTime(t *testing.T) {

	diagnostics := schema.NewDiagnostics()

	s := "2022-09-02 18:39:29"
	parse, err := time.Parse("2006-01-02 15:04:05", s)
	assert.Nil(t, err)
	foo := &struct {
		bar *time.Time
	}{
		bar: &parse,
	}

	extractValue, d := StructSelectorTime(".bar").Extract(context.Background(), nil, nil, nil, nil, nil, foo)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.False(t, reflect_util.IsNil(extractValue))
	assert.Equal(t, extractValue.(*time.Time).Format("2006-01-02 15:04:05"), s)

	foo2 := &struct {
		bar string
	}{
		bar: s,
	}
	extractValue, d = StructSelectorTime(".bar").Extract(context.Background(), nil, nil, nil, nil, nil, foo2)
	assert.False(t, diagnostics.Add(d).HasError())
	assert.False(t, reflect_util.IsNil(extractValue))
	assert.Equal(t, extractValue.(*time.Time).Format("2006-01-02 15:04:05"), s)

}
