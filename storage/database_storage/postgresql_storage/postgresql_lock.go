package postgresql_storage

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------------------------------- --------------------------------------------------------------------

var (
	ErrLockFailed        = errors.New("lock failed")
	ErrUnlockFailed      = errors.New("unlock failed")
	ErrLockNotFound      = errors.New("lock not found")
	ErrLockNotBelongYou  = errors.New("lock not belong you")
	ErrLockRefreshFailed = errors.New("lock refresh failed")
)

// ------------------------------------------------- --------------------------------------------------------------------

// LockInformation Some information about locks
type LockInformation struct {

	// Who holds the lock
	OwnerId string

	// Reentrant lock
	LockCount int

	// The expected expiration time of this lock
	ExceptedExpireTime time.Time
}

func FromJsonString(jsonString string) (*LockInformation, error) {
	r := &LockInformation{}
	err := json.Unmarshal([]byte(jsonString), r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (x *LockInformation) ToJsonString() string {
	marshal, err := json.Marshal(x)
	if err != nil {
		return ""
	} else {
		return string(marshal)
	}
}

// ------------------------------------------------- --------------------------------------------------------------------

const defaultCasRetryTimes = 3

func (x *PostgresqlStorage) Lock(ctx context.Context, lockId, ownerId string) error {
	return x.lockWithRetry(ctx, lockId, ownerId, defaultCasRetryTimes)
}

func (x *PostgresqlStorage) lockWithRetry(ctx context.Context, lockId, ownerId string, leftTryTimes int) error {
	lockKey := buildLockKey(lockId)

	// Determine whether the lock already exists
	information, _ := x.readLockInformation(ctx, lockKey)
	if information != nil {
		oldJsonString := information.ToJsonString()
		if information.OwnerId == ownerId {
			// Is reentrant to acquire the lock, increase the number of locks by 1
			information.LockCount++
			information.ExceptedExpireTime = x.nextExceptedExpireTime()
			// compare and set
			updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3 `
			rs, err := x.pool.Exec(ctx, updateSql, information.ToJsonString(), lockKey, oldJsonString)
			if err != nil {
				return err
			}
			// update success
			if rs.RowsAffected() != 0 {
				return nil
			}
			// need retry
			if leftTryTimes > 0 {
				return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
			}
			return ErrLockFailed
		} else {
			// If a lock exists but is not held by itself, check to see if it is an expired lock
			if information.ExceptedExpireTime.After(time.Now()) {
				// If the lock is not expired, it has to be abandoned
				return ErrLockFailed
			}
			// If the lock has expired, delete it and try to reacquire it
			dropExpiredLockSql := `DELETE FROM selefra_meta_kv WHERE key = $1 AND value = $2`
			rs, err := x.pool.Exec(ctx, dropExpiredLockSql, lockKey, oldJsonString)
			if err != nil {
				return err
			}
			// update failed, lock get failed
			if rs.RowsAffected() == 0 {
				return ErrLockFailed
			}
			if leftTryTimes > 0 {
				return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
			}
			return ErrLockFailed
		}
	}

	// The lock does not exist. Attempt to obtain the lock
	lockInformation := &LockInformation{
		OwnerId:   ownerId,
		LockCount: 1,
		// By default, a lock is expected to hold for at least ten minutes
		ExceptedExpireTime: x.nextExceptedExpireTime(),
	}
	sql := `INSERT INTO selefra_meta_kv (
                             "key",
                             "value" 
                             ) VALUES ( $1, $2 )`
	exec, err := x.pool.Exec(ctx, sql, lockKey, lockInformation.ToJsonString())
	if err != nil || exec.RowsAffected() != 1 {
		// lock failed
		return ErrLockFailed
	}

	// lock success, run refresh goroutine
	lock.Lock()
	defer lock.Unlock()
	goroutine := lockRefreshGoroutineMap[lockId]
	if goroutine != nil {
		goroutine.Stop()
	}
	refreshGoroutine := NewLockRefreshGoroutine(x, lockId, ownerId)
	refreshGoroutine.Start()
	lockRefreshGoroutineMap[lockId] = refreshGoroutine
	return nil
}

// refreshLockExpiredTime Refresh the expiration time of the lock you hold
func (x *PostgresqlStorage) refreshLockExpiredTime(ctx context.Context, lockId, ownerId string, exceptedExpiredTime time.Time) error {
	lockKey := buildLockKey(lockId)

	// Determine whether the lock already exists
	information, err := x.readLockInformation(ctx, lockKey)
	if err != nil {
		return err
	}
	// You can only refresh locks that you own
	if information.OwnerId != ownerId {
		return ErrLockNotBelongYou
	}
	oldJsonString := information.ToJsonString()
	information.ExceptedExpireTime = exceptedExpiredTime
	// compare and set
	updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3`
	rs, err := x.pool.Exec(ctx, updateSql, information.ToJsonString(), lockKey, oldJsonString)
	if err != nil {
		return err
	}
	if rs.RowsAffected() == 0 {
		return ErrLockRefreshFailed
	}
	return nil
}

// UnLock Release the lock, if it belongs to you
func (x *PostgresqlStorage) UnLock(ctx context.Context, lockId, ownerId string) error {
	return x.unlockWithRetry(ctx, lockId, ownerId, defaultCasRetryTimes)
}

// unlock operation may be failed by refresh goroutine, so need some retry
func (x *PostgresqlStorage) unlockWithRetry(ctx context.Context, lockId, ownerId string, tryTimes int) error {
	lockKey := buildLockKey(lockId)

	lockInformation, err := x.readLockInformation(ctx, lockKey)
	if err != nil {
		return err
	}
	// lock exists, check it's owner
	if lockInformation.OwnerId != ownerId {
		return ErrLockNotBelongYou
	}
	oldJsonString := lockInformation.ToJsonString()
	// ok, lock is mine, lock count - 1
	lockInformation.LockCount--
	if lockInformation.LockCount > 0 {
		// It is not released completely, but the count is reduced by 1 and updated back to the database
		// Is reentrant to acquire the lock, increase the number of locks by 1
		lockInformation.ExceptedExpireTime = x.nextExceptedExpireTime()
		// compare and set
		updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3 `
		rs, err := x.pool.Exec(ctx, updateSql, lockInformation.ToJsonString(), lockKey, oldJsonString)
		if err != nil {
			return err
		}
		// update success
		if rs.RowsAffected() != 0 {
			return nil
		}
		// update failed, need retry
		if tryTimes > 0 {
			return x.unlockWithRetry(ctx, lockId, ownerId, tryTimes-1)
		}
		// try times used up, finally failed
		return ErrUnlockFailed
	}

	// Once lock count is free, it needs to be completely free, which in this case means delete
	deleteSql := `DELETE FROM selefra_meta_kv WHERE key = $1 AND value = $2`
	exec, err := x.pool.Exec(ctx, deleteSql, lockKey, oldJsonString)
	if err != nil {
		return err
	}
	// delete failed
	if exec.RowsAffected() == 0 {
		// need retry
		if tryTimes > 0 {
			if err := x.unlockWithRetry(ctx, lockId, ownerId, tryTimes-1); err != nil {
				return err
			}
		} else {
			return ErrUnlockFailed
		}
	}

	// stop refresh goroutine
	lock.Lock()
	defer lock.Unlock()
	goroutine := lockRefreshGoroutineMap[lockId]
	if goroutine != nil {
		goroutine.Stop()
	}

	return nil
}

func (x *PostgresqlStorage) nextExceptedExpireTime() time.Time {
	return time.Now().Add(time.Minute * 10)
}

// read lock information from db
func (x *PostgresqlStorage) readLockInformation(ctx context.Context, lockKey string) (*LockInformation, error) {
	lockInformationJsonString, diagnostics := x.GetValue(ctx, lockKey)
	if diagnostics != nil && diagnostics.HasError() {
		return nil, ErrLockNotFound
	}
	return FromJsonString(lockInformationJsonString)
}

func buildLockKey(lockId string) string {
	return "storage_lock_id_" + lockId
}

// ------------------------------------------------- --------------------------------------------------------------------

var lock sync.RWMutex = sync.RWMutex{}
var lockRefreshGoroutineMap = make(map[string]*LockRefreshGoroutine)

type LockRefreshGoroutine struct {
	isRunning atomic.Bool
	storage   *PostgresqlStorage
	lockId    string
	ownerId   string
}

func NewLockRefreshGoroutine(storage *PostgresqlStorage, lockId, ownerId string) *LockRefreshGoroutine {
	return &LockRefreshGoroutine{
		isRunning: atomic.Bool{},
		storage:   storage,
		lockId:    lockId,
		ownerId:   ownerId,
	}
}

func (x *LockRefreshGoroutine) Start() {
	x.isRunning.Swap(true)
	go func() {
		for x.isRunning.Load() {
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*60)
			_ = x.storage.refreshLockExpiredTime(ctx, x.lockId, x.ownerId, time.Now().Add(time.Minute*10))
			// TODO log error & failed retry
			cancelFunc()
			time.Sleep(time.Second * 3)
		}
	}()
}

func (x *LockRefreshGoroutine) Stop() {
	x.isRunning.Store(false)
}

// ------------------------------------------------- --------------------------------------------------------------------
