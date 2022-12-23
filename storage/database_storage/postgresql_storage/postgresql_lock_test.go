package postgresql_storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPostgresqlStorage_Lock(t *testing.T) {
	err := testPostgresqlStorage.Lock(context.Background(), "test", "001")
	assert.Nil(t, err)
	time.Sleep(time.Second * 10)
	err = testPostgresqlStorage.UnLock(context.Background(), "test", "001")
	assert.Nil(t, err)
}

func TestPostgresqlStorage_RefreshLockExpiredTime(t *testing.T) {

}

func TestPostgresqlStorage_UnLock(t *testing.T) {
	err := testPostgresqlStorage.UnLock(context.Background(), "test", "001")
	assert.Nil(t, err)
}
