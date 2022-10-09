package column_value_extractor

import (
	"context"
	uuid2 "github.com/satori/go.uuid"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"strings"
)

type ColumnValueExtractorUUID struct {
	withoutHorizontal bool
}

var _ schema.ColumnValueExtractor = &ColumnValueExtractorUUID{}

func (x *ColumnValueExtractorUUID) Name() string {
	return "uuid-column-value-extractor"
}

func (x *ColumnValueExtractorUUID) Extract(ctx context.Context, clientMeta *schema.ClientMeta, client any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
	s := uuid2.NewV4().String()
	if x.withoutHorizontal {
		s = strings.ReplaceAll(s, "-", "")
	}
	return s, nil
}

func (x *ColumnValueExtractorUUID) DependencyColumnNames(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) []string {
	return nil
}

func (x *ColumnValueExtractorUUID) Validate(ctx context.Context, clientMeta *schema.ClientMeta, parentTable *schema.Table, table *schema.Table, column *schema.Column) *schema.Diagnostics {
	return nil
}

func UUID(withoutHorizontal ...bool) *ColumnValueExtractorUUID {
	b := true
	if len(withoutHorizontal) >= 1 {
		b = withoutHorizontal[0]
	}
	return &ColumnValueExtractorUUID{
		withoutHorizontal: b,
	}
}
