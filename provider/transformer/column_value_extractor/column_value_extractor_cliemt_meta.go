package column_value_extractor

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// ColumnValueExtractorClientMeta take item value from ClientMeta
type ColumnValueExtractorClientMeta struct {

	// the item name you want to take
	itemName string

	// if item name do not exist, you can give a default value
	defaultValue any
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorClientMeta{}

func (x *ColumnValueExtractorClientMeta) Name() string {
	return "client-meta-column-value-extractor"
}

func (x *ColumnValueExtractorClientMeta) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	if clientMeta == nil {
		return x.defaultValue, nil
	}
	itemValue := clientMeta.GetItem(x.itemName)
	if itemValue == nil {
		itemValue = x.defaultValue
	}
	return itemValue, nil
}

func (x *ColumnValueExtractorClientMeta) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorClientMeta) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	if clientMeta == nil {
		return schema.NewDiagnostics().AddErrorMsg(BuildExtractErrMsg(x, table, column, "ClientMeta is nil"))
	}
	return nil
}

func ClientMetaGetItem(itemName string) *ColumnValueExtractorClientMeta {
	return ClientMetaGetItemOrDefault(itemName, nil)
}

func ClientMetaGetItemOrDefault(itemName string, defaultValue any) *ColumnValueExtractorClientMeta {
	return &ColumnValueExtractorClientMeta{
		itemName:     itemName,
		defaultValue: defaultValue,
	}
}
