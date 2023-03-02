package postgresql_storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPostgresqlStorage_Lock(t *testing.T) {

	lockId := "test"
	ownerId := "001"

	err := testPostgresqlStorage.Lock(context.Background(), lockId, ownerId)
	assert.Nil(t, err)
	time.Sleep(time.Second * 3)
	err = testPostgresqlStorage.UnLock(context.Background(), lockId, ownerId)
	assert.Nil(t, err)
	time.Sleep(time.Second * 3)

	// no lock or lock not mime
	err = testPostgresqlStorage.UnLock(context.Background(), lockId, ownerId)
	assert.NotNil(t, err)

	// lock
	err = testPostgresqlStorage.Lock(context.Background(), lockId, ownerId)
	assert.Nil(t, err)

	// unlock
	err = testPostgresqlStorage.UnLock(context.Background(), lockId, ownerId)
	assert.Nil(t, err)

}

//func TestPostgresqlStorage_RefreshLockExpiredTime(t *testing.T) {
//
//}
//
//func TestPostgresqlStorage_UnLock(t *testing.T) {
//
//	lockId := "test"
//	ownerId := "001"
//
//	// no lock or lock not mime
//	err := testPostgresqlStorage.UnLock(context.Background(), lockId, ownerId)
//	assert.NotNil(t, err)
//
//	// lock
//	err = testPostgresqlStorage.Lock(context.Background(), lockId, ownerId)
//	assert.Nil(t, err)
//
//	// unlock
//	err = testPostgresqlStorage.UnLock(context.Background(), lockId, ownerId)
//	assert.Nil(t, err)
//
//}

func TestPostgresqlStorage_GetDatabaseTime(t *testing.T) {
	databaseTime, err := testPostgresqlStorage.GetTime(context.Background())
	assert.Nil(t, err)
	assert.False(t, databaseTime.IsZero())
}
