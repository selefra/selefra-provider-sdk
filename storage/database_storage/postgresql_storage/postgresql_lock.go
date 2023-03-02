package postgresql_storage

import (
	"context"
	"encoding/json"
	"errors"
	"go.uber.org/zap"
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

	x.DebugF("begin try lock", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))

	// Determine whether the lock already exists
	information, _ := x.readLockInformation(ctx, lockKey)
	if information != nil {
		oldJsonString := information.ToJsonString()

		x.DebugF("lock already exists", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.String("oldJsonString", oldJsonString))

		if information.OwnerId == ownerId {
			// Is reentrant to acquire the lock, increase the number of locks by 1
			information.LockCount++
			expireTime, err := x.nextExceptedExpireTime(ctx)
			if err != nil {
				x.ErrorF("get database time error", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
				if leftTryTimes > 0 {
					return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
				}
				return err
			}
			information.ExceptedExpireTime = expireTime
			// compare and set
			updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3 `
			rs, err := x.pool.Exec(ctx, updateSql, information.ToJsonString(), lockKey, oldJsonString)
			if err != nil {
				x.ErrorF("lock is mine, but exec cas for lock failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
				return err
			}
			// update success
			if rs.RowsAffected() != 0 {
				x.DebugF("lock is mine, exec cas for lock success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
				return nil
			}
			// need retry
			if leftTryTimes > 0 {
				x.DebugF("lock is mine, exec cas for lock miss, but i can retry", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
				return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
			}
			x.ErrorF("lock is mine, but exec cas for lock finally failed, and my try times used up, so give up", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			return ErrLockFailed
		} else {
			// If a lock exists but is not held by itself, check to see if it is an expired lock
			databaseTime, err := x.GetTime(ctx)
			if err != nil {
				x.ErrorF("get database time error", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
				if leftTryTimes > 0 {
					return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
				}
				return err
			}
			if information.ExceptedExpireTime.After(databaseTime) {
				x.ErrorF("lock is not mine and it is still ok, so i give up, ok, you win", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
				// If the lock is not expired, it has to be abandoned
				return ErrLockFailed
			}
			// If the lock has expired, delete it and try to reacquire it
			dropExpiredLockSql := `DELETE FROM selefra_meta_kv WHERE key = $1 AND value = $2`
			rs, err := x.pool.Exec(ctx, dropExpiredLockSql, lockKey, oldJsonString)
			if err != nil {
				x.ErrorF("lock is not mine and but it is expired, so i can kill it, but killed failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
				return err
			}
			// update failed, lock get failed
			if rs.RowsAffected() == 0 {
				x.ErrorF("lock is not mine and it is expired, so i can kill it, but killed failed, may be cas miss", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
				return ErrLockFailed
			}
			if leftTryTimes > 0 {
				x.ErrorF("lock is not mine and it is expired, so i can kill it, i killed success! woo, i will retry for lock", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
				return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
			}
			x.ErrorF("lock is not mine and it is expired, so i can kill it, i killed success! but my try times used up, so give up", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			return ErrLockFailed
		}
	}

	x.DebugF("lock not exists, try lock with cas", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))

	expireTime, err := x.nextExceptedExpireTime(ctx)
	if err != nil {
		x.ErrorF("get database time error", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
		if leftTryTimes > 0 {
			return x.lockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
		}
		return err
	}

	// The lock does not exist. Attempt to obtain the lock
	lockInformation := &LockInformation{
		OwnerId:   ownerId,
		LockCount: 1,
		// By default, a lock is expected to hold for at least ten minutes
		ExceptedExpireTime: expireTime,
	}
	sql := `INSERT INTO selefra_meta_kv (
                             "key",
                             "value" 
                             ) VALUES ( $1, $2 )`
	exec, err := x.pool.Exec(ctx, sql, lockKey, lockInformation.ToJsonString())
	if err != nil || exec.RowsAffected() != 1 {
		x.ErrorF("try cas lock failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
		// lock failed
		return ErrLockFailed
	}

	x.DebugF("try cas lock success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))

	// lock success, run refresh goroutine
	lock.Lock()
	defer lock.Unlock()
	goroutine := lockRefreshGoroutineMap[lockId]
	if goroutine != nil {
		x.DebugF("stop old refresh goroutine", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
		goroutine.Stop()
	}
	refreshGoroutine := NewLockRefreshGoroutine(x, lockId, ownerId)
	refreshGoroutine.Start()
	lockRefreshGoroutineMap[lockId] = refreshGoroutine
	x.DebugF("start new refresh goroutine", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
	return nil
}

// refreshLockExpiredTime Refresh the expiration time of the lock you hold
func (x *PostgresqlStorage) refreshLockExpiredTime(ctx context.Context, lockId, ownerId string, exceptedExpiredTime time.Time) error {
	lockKey := buildLockKey(lockId)

	// Determine whether the lock already exists
	information, err := x.readLockInformation(ctx, lockKey)
	if err != nil {
		x.ErrorF("try refresh, but read lock information failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Error(err))
		return err
	}
	oldJsonString := information.ToJsonString()
	// You can only refresh locks that you own
	if information.OwnerId != ownerId {
		x.ErrorF("try refresh, but lock not belong to you", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.String("oldJsonString", oldJsonString))
		return ErrLockNotBelongYou
	}
	information.ExceptedExpireTime = exceptedExpiredTime
	newJsonString := information.ToJsonString()
	// compare and set
	updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3`
	rs, err := x.pool.Exec(ctx, updateSql, newJsonString, lockKey, oldJsonString)
	if err != nil {
		x.ErrorF("try refresh, but cas failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.String("oldJsonString", oldJsonString), zap.Error(err))
		return err
	}
	if rs.RowsAffected() == 0 {
		x.ErrorF("try refresh, but cas miss", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.String("oldJsonString", oldJsonString), zap.Error(err))
		return ErrLockRefreshFailed
	}
	x.DebugF("try refresh, success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.String("oldJsonString", oldJsonString), zap.String("newJsonString", newJsonString))
	return nil
}

// UnLock Release the lock, if it belongs to you
func (x *PostgresqlStorage) UnLock(ctx context.Context, lockId, ownerId string) error {
	return x.unlockWithRetry(ctx, lockId, ownerId, defaultCasRetryTimes)
}

// unlock operation may be failed by refresh goroutine, so need some retry
func (x *PostgresqlStorage) unlockWithRetry(ctx context.Context, lockId, ownerId string, leftTryTimes int) error {
	lockKey := buildLockKey(lockId)

	lockInformation, err := x.readLockInformation(ctx, lockKey)
	if err != nil {
		x.ErrorF("try unlock, but lock not exists", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
		return err
	}
	oldJsonString := lockInformation.ToJsonString()
	// lock exists, check it's owner
	if lockInformation.OwnerId != ownerId {
		x.ErrorF("try unlock, but lock not belong to you", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.String("oldJsonString", oldJsonString))
		return ErrLockNotBelongYou
	}

	x.DebugF("try unlock, lock exists and is belong me", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.String("oldJsonString", oldJsonString))

	// ok, lock is mine, lock count - 1
	lockInformation.LockCount--
	if lockInformation.LockCount > 0 {
		// It is not released completely, but the count is reduced by 1 and updated back to the database
		// Is reentrant to acquire the lock, increase the number of locks by 1
		expireTime, err := x.nextExceptedExpireTime(ctx)
		if err != nil {
			x.ErrorF("get database time error", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
			if leftTryTimes > 0 {
				return x.unlockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
			}
			return err
		}
		lockInformation.ExceptedExpireTime = expireTime
		// compare and set
		updateSql := `UPDATE selefra_meta_kv SET value = $1 WHERE key = $2 AND value = $3 `
		rs, err := x.pool.Exec(ctx, updateSql, lockInformation.ToJsonString(), lockKey, oldJsonString)
		if err != nil {
			x.ErrorF("try unlock, after unlock still hold lock, cas failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
			return err
		}
		// update success
		if rs.RowsAffected() != 0 {
			x.DebugF("try unlock, after unlock still hold lock, unlock success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			return nil
		}
		// update failed, need retry
		if leftTryTimes > 0 {
			x.ErrorF("try unlock, after unlock still hold lock, cas miss, but i can retry", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			return x.unlockWithRetry(ctx, lockId, ownerId, leftTryTimes-1)
		}
		x.ErrorF("try unlock, after unlock still hold lock, and i try times used up, so i give up", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
		// try times used up, finally failed
		return ErrUnlockFailed
	}

	// Once lock count is free, it needs to be completely free, which in this case means delete
	deleteSql := `DELETE FROM selefra_meta_kv WHERE key = $1 AND value = $2`
	exec, err := x.pool.Exec(ctx, deleteSql, lockKey, oldJsonString)
	if err != nil {
		x.ErrorF("try unlock, lock need release, but cas failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
		return err
	}
	// delete failed
	if exec.RowsAffected() == 0 {
		// need retry
		if leftTryTimes > 0 {
			x.ErrorF("try unlock, and lock need release, cas miss, but i can retry", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			if err := x.unlockWithRetry(ctx, lockId, ownerId, leftTryTimes-1); err != nil {
				x.ErrorF("try unlock, and lock need release, retry failed", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes), zap.Error(err))
				return err
			}
			x.DebugF("try unlock, and lock need release, retry success, release success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
		} else {
			x.ErrorF("try unlock, and lock need release, and cas miss, and i try times used up, so give up", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
			return ErrUnlockFailed
		}
	} else {
		x.DebugF("try unlock, and lock need release, cas success", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
	}

	// stop refresh goroutine
	lock.Lock()
	defer lock.Unlock()
	goroutine := lockRefreshGoroutineMap[lockId]
	if goroutine != nil {
		x.DebugF("try unlock, send refresh goroutine stop signal", zap.String("lockId", lockId), zap.String("ownerId", ownerId), zap.Int("leftTryTimes", leftTryTimes))
		goroutine.Stop()
	}

	return nil
}

func (x *PostgresqlStorage) nextExceptedExpireTime(ctx context.Context) (time.Time, error) {
	databaseTime, err := x.GetTime(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return databaseTime.Add(time.Minute * 10), nil
}

//func (x *PostgresqlStorage) x.GetTime(ctx context.Context) (time.Time, error) {
//	var zero time.Time
//	sql := `SELECT NOW()`
//	rs, err := x.pool.Query(ctx, sql)
//	if err != nil {
//		return zero, err
//	}
//	defer func() {
//		rs.Close()
//	}()
//	if !rs.Next() {
//		return zero, errors.New("can not query database time")
//	}
//	var dbTime time.Time
//	err = rs.Scan(&dbTime)
//	if err != nil {
//		return zero, err
//	}
//	return dbTime, nil
//}

// read lock information from db
func (x *PostgresqlStorage) readLockInformation(ctx context.Context, lockKey string) (*LockInformation, error) {
	lockInformationJsonString, diagnostics := x.GetValue(ctx, lockKey)
	if diagnostics != nil && diagnostics.HasError() {
		return nil, ErrLockNotFound
	}
	return FromJsonString(lockInformationJsonString)
}

func (x *PostgresqlStorage) DebugF(msg string, args ...any) {
	if x.clientMeta != nil {
		x.clientMeta.DebugF(msg, args...)
	}
}

func (x *PostgresqlStorage) ErrorF(msg string, args ...any) {
	if x.clientMeta != nil {
		x.clientMeta.ErrorF(msg, args...)
	}
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
		continueFailedCount := 0
		for x.isRunning.Load() {

			// query database time
			ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*60)
			databaseTime, err := x.storage.GetTime(ctx)
			cancelFunc()
			if err != nil {
				x.storage.ErrorF("get database time error", zap.Error(err))
				time.Sleep(time.Second * 10)
				continue
			}

			ctx, cancelFunc = context.WithTimeout(context.Background(), time.Second*60)
			err = x.storage.refreshLockExpiredTime(ctx, x.lockId, x.ownerId, databaseTime.Add(time.Minute*10))
			cancelFunc()

			if err != nil {
				continueFailedCount++
				if continueFailedCount > 10 {
					x.storage.DebugF("lock refresh continue failed too many, go goroutine exit", zap.String("lockId", x.lockId), zap.String("ownerId", x.ownerId))
					return
				}
			} else {
				continueFailedCount = 0
			}
			time.Sleep(time.Second * 3)
		}
		x.storage.DebugF("lock refresh go goroutine exit", zap.String("lockId", x.lockId), zap.String("ownerId", x.ownerId))
	}()
}

func (x *LockRefreshGoroutine) Stop() {
	x.isRunning.Store(false)
}

// ------------------------------------------------- --------------------------------------------------------------------
