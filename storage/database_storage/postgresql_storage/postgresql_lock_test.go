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
	time.Sleep(time.Second * 30)
	err = testPostgresqlStorage.UnLock(context.Background(), "test", "001")
	assert.Nil(t, err)
	time.Sleep(time.Second * 10)

	//for i := 0; i < 100; i++ {
	//	err := testPostgresqlStorage.Lock(context.Background(), "test", "001")
	//	if err != nil {
	//		fmt.Println(err)
	//	} else {
	//		fmt.Println(fmt.Sprintf("%d: lock success", i))
	//	}
	//}

}

func TestPostgresqlStorage_RefreshLockExpiredTime(t *testing.T) {

}

func TestPostgresqlStorage_UnLock(t *testing.T) {
	err := testPostgresqlStorage.UnLock(context.Background(), "test", "001")
	assert.Nil(t, err)
}
