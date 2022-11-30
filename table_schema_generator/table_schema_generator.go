package table_schema_generator

import (
	"context"

	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

type TableSchemaGenerator interface {
	GetTableName() string

	GetTableDescription() string

	GetColumns() []*schema.Column

	GetSubTables() []*schema.Table

	GetOptions() *schema.TableOptions

	GetDataSource() *schema.DataSource

	GetExpandClientTask() func(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask) []*schema.ClientTaskContext

	GetVersion() uint64
}

func GenTableSchema(g TableSchemaGenerator) *schema.Table {
	return &schema.Table{
		TableName:        g.GetTableName(),
		Description:      g.GetTableDescription(),
		Columns:          g.GetColumns(),
		SubTables:        g.GetSubTables(),
		Options:          g.GetOptions(),
		DataSource:       *g.GetDataSource(),
		ExpandClientTask: g.GetExpandClientTask(),
		Version:          g.GetVersion(),
	}
}
