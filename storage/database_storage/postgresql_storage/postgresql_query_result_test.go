package postgresql_storage

import (
	"context"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-utils/pkg/json_util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPostgresqlQueryResult_ReadRowSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	storage, err := NewPostgresqlStorage(ctx, NewPostgresqlStorageOptions(env.GetDatabaseDsn()))
	assert.Nil(t, err)

	queryResult, err := storage.Query(ctx, "SELECT * FROM pg_class c,pg_attribute a,pg_type t\nWHERE c.relname = 'user_test' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid\nORDER BY a.attnum;")
	assert.Nil(t, err)

	rowSet, err := queryResult.ReadRows(-1)
	assert.Nil(t, err)

	fmt.Println(json_util.ToJsonString(rowSet))

}
