package provider

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
	"github.com/selefra/selefra-utils/pkg/id_util"
	"github.com/selefra/selefra-utils/pkg/pointer"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func Test_resultHandler(t *testing.T) {

	diagnostics := schema.NewDiagnostics()

	// struct --> table schema
	type Foo struct {
		Bar1 string
		Bar2 int
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()
	table := &schema.Table{
		TableName: "test_table_for_provider_runtime_result_handler",
		Columns: []*schema.Column{
			&schema.Column{
				ColumnName: "bar1",
				Type:       schema.ColumnTypeString,
				Extractor:  column_value_extractor.StructSelector("Bar1"),
			},
			&schema.Column{
				ColumnName: "bar2",
				Type:       schema.ColumnTypeInt,
				Extractor:  column_value_extractor.StructSelector("Bar2"),
			},
		},
	}
	provider := Provider{
		Name:    "test-provider",
		Version: "v0.1",
		TableList: []*schema.Table{
			table,
		},
		TransformerMeta: schema.TransformerMeta{
			DataSourcePullResultAutoExpand: true,
		},
	}

	// init
	options := postgresql_storage.NewPostgresqlStorageOptions(env.GetDatabaseDsn())
	jsonString, err := options.ToJsonString()
	assert.Nil(t, err)
	initRequest := &shard.ProviderInitRequest{
		Storage: &shard.Storage{
			Type:           shard.POSTGRESQL,
			StorageOptions: []byte(jsonString),
		},
		Workspace:     pointer.ToStringPointer("./"),
		IsInstallInit: pointer.TruePointer(),
	}
	initResponse, err := provider.Init(ctx, initRequest)
	assert.Nil(t, err)
	assert.NotNil(t, initResponse)
	if initResponse.Diagnostics != nil && initResponse.Diagnostics.Size() != 0 {
		t.Logf("init provider: %s", initResponse.Diagnostics.ToString())
	}
	assert.False(t, diagnostics.AddDiagnostics(initResponse.Diagnostics).HasError())

	// call result handler
	// single
	task := &schema.DataSourcePullTask{
		TaskId: id_util.RandomId(),
		Ctx:    ctx,
		Table:  table,
	}
	foo := &Foo{
		Bar1: "80",
		Bar2: 443,
	}
	rows, slice, d := provider.runtime.resultHandler(ctx, &provider.ClientMeta, nil, task, foo)
	if d != nil && d.Size() != 0 {
		t.Logf("single result: %s", d.ToString())
	}
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	assert.NotNil(t, rows)
	assert.Equal(t, 1, rows.RowCount())
	assert.Equal(t, "80", rows.GetCellStringValueOrDefault(0, 0, ""))
	assert.Equal(t, 443, rows.GetCellIntValueOrDefault(0, 1, 0))
	assert.Equal(t, rows.RowCount(), len(slice))

	// multi result
	fooSlice := make([]*Foo, 0)
	for i := 0; i < 10; i++ {
		fooSlice = append(fooSlice, &Foo{
			Bar1: strconv.Itoa(i),
			Bar2: i,
		})
	}
	rows, slice, d = provider.runtime.resultHandler(ctx, &provider.ClientMeta, nil, task, fooSlice)
	if d != nil && d.Size() != 0 {
		t.Logf("multi result: %s", d.ToString())
	}
	assert.False(t, diagnostics.AddDiagnostics(d).HasError())
	assert.NotNil(t, rows)
	assert.Equal(t, 10, rows.RowCount())
	assert.Equal(t, "9", rows.GetCellStringValueOrDefault(9, 0, ""))
	assert.Equal(t, 9, rows.GetCellIntValueOrDefault(9, 1, 0))
	assert.Equal(t, rows.RowCount(), len(slice))

}
