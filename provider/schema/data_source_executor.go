package schema

import (
	"context"
	"fmt"
	"github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/selefra/selefra-utils/pkg/id_util"
	"github.com/selefra/selefra-utils/pkg/reflect_util"
	"github.com/selefra/selefra-utils/pkg/runtime_util"
	"strings"
	"sync"
	"time"
)

// The main thing here is to encapsulate some logic for pulling data concurrently

// DataSourceExecutor Pulls the actuator of the data source
type DataSourceExecutor struct {

	// The pull data task waiting to be executed
	taskQueue *DataSourcePullTaskQueue

	clientMeta *ClientMeta

	errorsHandlerMeta *ErrorsHandlerMeta

	// Number of worker concurrency, the maximum number of workers working at the same time
	workerNum uint64
	// Concurrency control wait
	wg *sync.WaitGroup
}

// DefaultWorkerNum The default number of threads when the number of worker threads is not specified
var DefaultWorkerNum uint64 = 1

// NewDataSourcePullExecutor Create a data source data pull actuator
// @params: workerNum Number of working coroutines
// @wg: Concurrency control
func NewDataSourcePullExecutor(workerNum uint64, clientMeta *ClientMeta, errorsHandlerMeta *ErrorsHandlerMeta) (*DataSourceExecutor, *Diagnostics) {

	diagnostics := NewDiagnostics()

	if workerNum <= 0 {
		workerNum = DefaultWorkerNum
		diagnostics.AddInfo("unset data source executor worker num, so give it default value: %d", DefaultWorkerNum)
	}

	executor := &DataSourceExecutor{
		clientMeta:        clientMeta,
		errorsHandlerMeta: errorsHandlerMeta,
		workerNum:         workerNum,
		taskQueue:         NewDataSourcePullTaskQueue(),
		wg:                &sync.WaitGroup{},
	}

	// The worker pool is started when created
	executor.runWorkers()

	return executor, diagnostics
}

// Submit Submit a data source pull task for execution
func (x *DataSourceExecutor) Submit(ctx context.Context, task *DataSourcePullTask) *Diagnostics {
	x.taskQueue.Add(task)
	return nil
}

// ShutdownAndAwaitTermination Close the task queue and hold the current coroutine until the task completes or times out
func (x *DataSourceExecutor) ShutdownAndAwaitTermination(ctx context.Context) *Diagnostics {

	x.wg.Wait()

	return nil
}

func (x *DataSourceExecutor) runWorkers() {

	semaphore := NewConsumerSemaphore()

	for i := uint64(1); i <= x.workerNum; i++ {
		x.wg.Add(1)

		consumerId := i
		semaphore.Init(consumerId)

		go func() {

			defer x.wg.Done()

			for !semaphore.IsAllConsumerDone() {

				if task := x.taskQueue.Take(); task != nil {

					semaphore.Running(consumerId)

					x.execTask(task)

					// Callback method after task completion, if any
					if task.TaskDoneCallback != nil {
						task.TaskDoneCallback(task.Ctx, x.clientMeta, task)
					}

				} else {
					semaphore.Idle(consumerId)
					time.Sleep(time.Second * 1)
				}
			}

		}()
	}
}

// Tasks may generate new tasks, which are executed recursively
func (x *DataSourceExecutor) execTask(task *DataSourcePullTask) {

	taskId := task.TaskId
	table := task.Table
	taskBegin := time.Now()
	isIgnorePullTableError := x.errorsHandlerMeta.IsIgnore(IgnoredErrorOnPullTable)

	x.clientMeta.DebugF("taskId = %s, begin execution", taskId)

	wg := sync.WaitGroup{}

	// just init client task context if it is not
	if !task.IsExpandDone {
		x.expandTask(context.Background(), task)
		return
	}

	resultChannel := make(chan any, 10000)

	// step 1. Start a coroutine that pulls data
	// TODO The size of the channel that receives the result is determined dynamically
	wg.Add(1)
	go func() {

		defer func() {
			if err := recover(); err != nil {

				msg := strings.Builder{}
				msg.WriteString(fmt.Sprintf("taskId = %s, table %s data source pull table panic: %s", taskId, table.TableName, err))
				if !isIgnorePullTableError {
					task.DiagnosticsChannel <- NewDiagnostics().AddErrorMsg(msg.String())
				}

				if task.ParentRow != nil {
					msg.WriteString(fmt.Sprintf("\n parent row:  %s \n", task.ParentRow.String()))
				}
				if task.ParentRawResult != nil {
					msg.WriteString(fmt.Sprintf("\n parent raw result:  %s \n", task.ParentRawResult))
				}
				msg.WriteString("\nStack: \n")
				msg.WriteString(runtime_util.Stack())
				x.clientMeta.Error(msg.String())

			}
			// close result channel
			close(resultChannel)
			wg.Done()

			x.clientMeta.DebugF("taskId = %s, pull table done", taskId)
		}()

		clientBegin := time.Now()
		x.clientMeta.DebugF("taskId = %s, begin execution pull table...", taskId)
		d := task.Table.DataSource.Pull(context.Background(), x.clientMeta, task.Client, task, resultChannel)

		clientCost := time.Now().Sub(clientBegin)
		x.clientMeta.DebugF("taskId = %s, execution pull table done, cost = %s", taskId, clientCost.String())

		// send diagnostics if not ignore error
		if x.errorsHandlerMeta.IsIgnore(IgnoredErrorOnPullTable) {
			return
		} else if d != nil {
			task.DiagnosticsChannel <- d
		}

	}()

	// step 2. Start the coroutine that processes the pulled data
	wg.Add(1)
	go func() {

		defer func() {
			if err := recover(); err != nil {

				msg := strings.Builder{}
				msg.WriteString(fmt.Sprintf("taskId = %s, table %s data source pull table, handle result panic: %s", taskId, table.TableName, err))
				if !isIgnorePullTableError {
					task.DiagnosticsChannel <- NewDiagnostics().AddErrorMsg(msg.String())
				}

				if task.ParentRow != nil {
					msg.WriteString(fmt.Sprintf("\n parent row: %s\n", task.ParentRow.String()))
				}
				if task.ParentRawResult != nil {
					msg.WriteString(fmt.Sprintf("\n parent raw result: %s\n", task.ParentRawResult))
				}
				msg.WriteString("\nStack: \n")
				msg.WriteString(runtime_util.Stack())
				x.clientMeta.Error(msg.String())

			}
			wg.Done()

		}()

		// collect result
		for result := range resultChannel {

			// drop nil result
			if reflect_util.IsNil(result) {
				x.clientMeta.DebugF("taskId = %s, return nil result, ignored it", taskId)
				continue
			}

			// run task result handler
			rows, resultSlice, d := x.execResultHandlerWithRecover(task.Ctx, x.clientMeta, task.Client, task, result)
			if d != nil && d.HasError() {
				if !isIgnorePullTableError {
					task.DiagnosticsChannel <- d
				}
			} else {
				task.DiagnosticsChannel <- d
			}
			if rows == nil || rows.IsEmpty() {
				x.clientMeta.DebugF("taskId = %s, task result handler return nil rows", taskId)
				continue
			}

			// The current table parsed to the result of matrix transformation, and divided into a number of only one row of matrices
			rowSlice := rows.SplitRowByRow()
			if len(rowSlice) != len(resultSlice) {
				x.clientMeta.ErrorF("taskId = %s, len(rowSlice) != len(resultSlice)", taskId)
				continue
			}
			for i := 0; i < len(rowSlice); i++ {
				row := rowSlice[i]
				result := resultSlice[i]
				// Start a data pull task for each child table
				for _, subTable := range task.Table.SubTables {
					subTask := &DataSourcePullTask{

						TaskId: id_util.RandomId(),
						Ctx:    task.Ctx,

						ParentTask:      task,
						ParentTable:     task.Table,
						ParentRow:       row,
						ParentRawResult: result,

						Table:              subTable,
						ResultHandler:      task.ResultHandler,
						TaskDoneCallback:   task.TaskDoneCallback,
						DiagnosticsChannel: task.DiagnosticsChannel,

						IsRootTask:   false,
						IsExpandDone: true,
						Client:       task.Client,
					}
					x.clientMeta.DebugF("taskId = %s, start subTaskId = %s, parent row = %s, parent raw result = %s", subTask.TaskId, row, result)
					x.Submit(context.Background(), subTask)
				}
			}
		}
	}()

	// Waiting for the two of you to finish
	wg.Wait()

	taskCost := time.Now().Sub(taskBegin)
	x.clientMeta.DebugF("taskId = %s, execution done, cost = %s", taskId, taskCost.String())
}

// Expand the task, initialize the relevant task context, and so on
func (x *DataSourceExecutor) expandTask(ctx context.Context, task *DataSourcePullTask) {

	taskId := task.TaskId

	x.clientMeta.DebugF("taskId = %s, begin expand...", taskId)

	// compute client for execution task
	clientSlice := make([]any, 0)
	if len(x.clientMeta.GetClientSlice()) != 0 {
		// just root task use all client
		clientSlice = append(clientSlice, x.clientMeta.GetClientSlice()...)
	} else {
		clientSlice = append(clientSlice, nil)
	}

	// Create the client task execution context
	clientTaskContextSlice := make([]*ClientTaskContext, 0)
	for _, client := range clientSlice {
		// expand task if necessary
		if task.Table != nil && task.Table.ExpandClientTask != nil {
			for _, clientTaskContext := range task.Table.ExpandClientTask(task.Ctx, x.clientMeta, client, task) {
				// You can omit the task field, will use default task's clone
				if clientTaskContext.Task == nil {
					clientTaskContext.Task = task.Clone()
				}
				clientTaskContextSlice = append(clientTaskContextSlice, clientTaskContext)
			}
		} else {
			clientTaskContextSlice = append(clientTaskContextSlice, &ClientTaskContext{
				Client: client,
				Task:   task.Clone(),
			})
		}
	}
	x.clientMeta.DebugF("taskId = %s, client task context create done, expand task count = %d", taskId, len(clientTaskContextSlice))
	if len(clientTaskContextSlice) == 0 {
		x.clientMeta.DebugF("taskId = %s, client task count equal zero, so ignored", taskId)
		return
	}

	// send new task
	for _, clientTaskContext := range clientTaskContextSlice {
		expandTask := clientTaskContext.Task
		// generate new task id
		expandTask.TaskId = expandTask.TaskId + "-" + id_util.RandomId()
		expandTask.Client = clientTaskContext.Client
		expandTask.IsExpandDone = true
		x.Submit(ctx, expandTask)
	}

}

// Perform the task completion callback while capturing Panic
func (x *DataSourceExecutor) execResultHandlerWithRecover(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, result any) (rows *Rows, resultSlice []any, diagnostics *Diagnostics) {

	diagnostics = NewDiagnostics()

	defer func() {

		if err := recover(); err != nil {

			// force drop result when panic
			rows = nil

			msg := strings.Builder{}
			msg.WriteString(fmt.Sprintf("taskId = %s, exec result handler panic: %s", task.TaskId, err))
			if !x.errorsHandlerMeta.IsIgnore(IgnoredErrorOnPullTable) {
				diagnostics.AddErrorMsg(msg.String())
			}

			msg.WriteString(fmt.Sprintf("\n result:  %s \n", result))
			if task.ParentRow != nil {
				msg.WriteString(fmt.Sprintf("\n parent row:  %s \n", task.ParentRow.String()))
			}
			if task.ParentRawResult != nil {
				msg.WriteString(fmt.Sprintf("\n parent raw result:  %s \n", task.ParentRawResult))
			}
			msg.WriteString("\nStack: \n")
			msg.WriteString(runtime_util.Stack())
			x.clientMeta.Error(msg.String())

		}

	}()

	// A task may have many results. In order to avoid a single instance affecting each other, creating a copy of the
	// task is equivalent to isolating the task context of the different results
	task = task.Clone()
	task.NotExpandRawResult = result
	rows, resultSlice, diagnostics = task.ResultHandler(ctx, x.clientMeta, client, task, result)
	return
}

// ---------------------------------------------------------------------------------------------------------------------

// DataSourcePullTaskQueue A dedicated task queue allows you to expand the task queue at will
type DataSourcePullTaskQueue struct {
	lock sync.RWMutex
	list *singlylinkedlist.List
}

func NewDataSourcePullTaskQueue() *DataSourcePullTaskQueue {
	return &DataSourcePullTaskQueue{
		lock: sync.RWMutex{},
		list: &singlylinkedlist.List{},
	}
}

func (x *DataSourcePullTaskQueue) Add(task *DataSourcePullTask) {
	x.lock.Lock()
	defer x.lock.Unlock()

	x.list.Add(task)
}

func (x *DataSourcePullTaskQueue) Take() *DataSourcePullTask {
	x.lock.Lock()
	defer x.lock.Unlock()

	value, ok := x.list.Get(0)
	if ok {
		x.list.Remove(0)
		return value.(*DataSourcePullTask)
	} else {
		return nil
	}
}

func (x *DataSourcePullTaskQueue) IsEmpty() bool {
	x.lock.RLock()
	defer x.lock.RUnlock()

	return x.list.Empty()
}

// ---------------------------------------------------------------------------------------------------------------------

// ConsumerSemaphore Used to coordinate the work and exit of all consumers
type ConsumerSemaphore struct {
	lock                 sync.RWMutex
	consumerIdleCountMap map[uint64]int
}

func NewConsumerSemaphore() *ConsumerSemaphore {
	return &ConsumerSemaphore{
		lock:                 sync.RWMutex{},
		consumerIdleCountMap: make(map[uint64]int),
	}
}

func (x *ConsumerSemaphore) Init(consumerId uint64) {
	x.lock.Lock()
	defer x.lock.Unlock()

	x.consumerIdleCountMap[consumerId] = 0
}

func (x *ConsumerSemaphore) Running(consumerId uint64) {
	x.lock.Lock()
	defer x.lock.Unlock()

	x.consumerIdleCountMap[consumerId] = 0
}

func (x *ConsumerSemaphore) Idle(consumerId uint64) {
	x.lock.Lock()
	defer x.lock.Unlock()

	idleCount, exists := x.consumerIdleCountMap[consumerId]
	if exists {
		idleCount += 1
	} else {
		idleCount = 1
	}
	x.consumerIdleCountMap[consumerId] = idleCount
}

func (x *ConsumerSemaphore) IsAllConsumerDone() bool {
	x.lock.RLock()
	defer x.lock.RUnlock()

	for _, idleCount := range x.consumerIdleCountMap {
		if idleCount < 3 {
			return false
		}
	}

	return true
}

// ---------------------------------------------------------------------------------------------------------------------
