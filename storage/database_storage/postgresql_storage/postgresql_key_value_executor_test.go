package postgresql_storage

import (
	"context"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_ensureKeyValueTableExists(t *testing.T) {

}

func TestPostgresqlCRUDExecutor_SetKey(t *testing.T) {

	diagnostics := schema.NewDiagnostics()

	d := testKeyValueExecutor.SetKey(context.Background(), "test_key", "test_value")
	assert.False(t, diagnostics.Add(d).HasError())
	if diagnostics.HasError() {
		t.Log(diagnostics.ToString())
	}

	value, d := testKeyValueExecutor.GetValue(context.Background(), "test_key")
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, "test_value", value)
	if diagnostics.HasError() {
		t.Log(diagnostics.ToString())
	}

	d = testKeyValueExecutor.SetKey(context.Background(), "test_key", "test_value_update")
	assert.True(t, d == nil || !d.HasError())
	if diagnostics.HasError() {
		t.Log(diagnostics.ToString())
	}

	value, d = testKeyValueExecutor.GetValue(context.Background(), "test_key")
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, "test_value_update", value)
	if diagnostics.HasError() {
		t.Log(diagnostics.ToString())
	}
}

func TestPostgresqlKeyValueExecutor_DeleteKey(t *testing.T) {

	diagnostics := schema.NewDiagnostics()

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	d := testKeyValueExecutor.SetKey(ctx, "test_key", "test_value")
	cancelFunc()
	assert.False(t, diagnostics.Add(d).HasError())

	ctx, cancelFunc = context.WithTimeout(context.Background(), time.Second*30)
	value, d := testKeyValueExecutor.GetValue(context.Background(), "test_key")
	cancelFunc()
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, "test_value", value)

	ctx, cancelFunc = context.WithTimeout(context.Background(), time.Second*30)
	d = testKeyValueExecutor.DeleteKey(context.Background(), "test_key")
	cancelFunc()
	assert.False(t, diagnostics.Add(d).HasError())

	ctx, cancelFunc = context.WithTimeout(context.Background(), time.Second*30)
	value, d = testKeyValueExecutor.GetValue(context.Background(), "test_key")
	cancelFunc()
	assert.False(t, diagnostics.Add(d).HasError())
	assert.Equal(t, "", value)

}

func TestPostgresqlKeyValueExecutor_ListKey(t *testing.T) {

	diagnostics := schema.NewDiagnostics()

	// clear
	rows, d := testKeyValueExecutor.ListKey(context.Background())
	assert.False(t, diagnostics.Add(d).HasError())
	for i := 0; i < rows.RowCount(); i++ {
		row, err := rows.GetRow(i)
		assert.Nil(t, err)
		key, err := row.GetString("key")
		assert.Nil(t, err)
		testKeyValueExecutor.DeleteKey(context.Background(), key)
	}

	d = testKeyValueExecutor.SetKey(context.Background(), "test_key", "test_value")
	assert.False(t, diagnostics.Add(d).HasError())

	d = testKeyValueExecutor.SetKey(context.Background(), "test_key_002", "test_value_002")
	assert.False(t, diagnostics.Add(d).HasError())

	rows, d = testKeyValueExecutor.ListKey(context.Background())
	assert.False(t, diagnostics.Add(d).HasError())
	assert.NotNil(t, rows)
	assert.Equal(t, 2, rows.RowCount())
}
