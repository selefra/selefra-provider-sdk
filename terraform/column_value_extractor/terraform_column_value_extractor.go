package column_value_extractor

import (
	"context"
	"github.com/hashicorp/go-cty/cty"
	"github.com/hashicorp/go-cty/cty/json"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-utils/pkg/json_util"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/songzhibin97/go-ognl"
	"reflect"
)

func TerraformRawDataColumnValueExtractor(selectors ...string) schema.ColumnValueExtractor {
	return column_value_extractor.WrapperExtractFunction(func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
		if reflect_util.IsNil(result) {
			return nil, nil
		}
		if len(selectors) != 0 {
			for _, selector := range selectors {
				v := ognl.Get(result, selector).Value()
				if !reflect_util.IsNil(v) {
					return json_util.ToJsonString(EnsureJSONSerializable(v)), nil
				}
			}
			return nil, nil
		} else {
			return json_util.ToJsonString(EnsureJSONSerializable(result)), nil
		}
	})
}

// EnsureJSONSerializable Because some of the types are custom to hashicorp, we need to deal with them first
// https://github.com/hashicorp/go-cty/blob/master/docs/json.md
func EnsureJSONSerializable(result any) any {

	switch _type := result.(type) {
	case cty.Value:
		return json.SimpleJSONValue{Value: _type}
	}

	of := reflect.ValueOf(result)
	switch of.Kind() {
	case reflect.Map:
		for _, key := range of.MapKeys() {
			value := of.MapIndex(key)
			of.SetMapIndex(key, reflect.ValueOf(EnsureJSONSerializable(value.Interface())))
		}
	}
	return result
}
