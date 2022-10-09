package schema

import (
	"context"
	"fmt"
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
	taskChannel chan *DataSourcePullTask

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
		// TODO Perhaps the task queue size should be calculated dynamically?
		taskChannel: make(chan *DataSourcePullTask, 1000),
		wg:          &sync.WaitGroup{},
	}

	// The worker pool is started when created
	executor.runWorkers()

	return executor, diagnostics
}

// Submit Submit a data source pull task for execution
func (x *DataSourceExecutor) Submit(ctx context.Context, task *DataSourcePullTask) *Diagnostics {
	for {
		select {
		case x.taskChannel <- task:
			return nil
		case <-ctx.Done():
			return NewDiagnostics().AddErrorMsg("table %s pull request id %s submit context timeout", task.Table.TableName, task.TaskId)
		}
	}
}

// ShutdownAndAwaitTermination Close the task queue and hold the current coroutine until the task completes or times out
func (x *DataSourceExecutor) ShutdownAndAwaitTermination(ctx context.Context) *Diagnostics {

	close(x.taskChannel)

	x.wg.Wait()

	return nil
}

func (x *DataSourceExecutor) runWorkers() {
	for workerId := uint64(1); workerId <= x.workerNum; workerId++ {
		x.wg.Add(1)
		go func() {
			defer x.wg.Done()
			for task := range x.taskChannel {
				x.execTask(task, nil, true)

				// Callback method after task completion, if any
				if task.TaskDoneCallback != nil {
					task.TaskDoneCallback(task.Ctx, x.clientMeta, task)
				}
			}
		}()
	}
}

// Tasks may generate new tasks, which are executed recursively
func (x *DataSourceExecutor) execTask(task *DataSourcePullTask, client any, isRootTask bool) {

	taskId := task.TaskId
	table := task.Table
	taskBegin := time.Now()
	isIgnorePullTableError := x.errorsHandlerMeta.IsIgnore(IgnoredErrorOnPullTable)

	x.clientMeta.DebugF("taskId = %s, begin execution", taskId)

	wg := sync.WaitGroup{}

	// compute client for execution task
	clientSlice := make([]any, 0)
	if isRootTask {
		if len(x.clientMeta.GetClientSlice()) != 0 {
			// just root task use all client
			clientSlice = append(clientSlice, x.clientMeta.GetClientSlice()...)
		} else {
			clientSlice = append(clientSlice, nil)
		}
	} else {
		// just use parent give me client
		clientSlice = append(clientSlice, client)
	}

	// Create the client task execution context
	clientTaskContextSlice := make([]*ClientTaskContext, 0)
	for _, client := range clientSlice {
		// expand task if necessary
		if task.Table != nil && task.Table.ExpandClientTask != nil {
			for _, clientTaskContext := range task.Table.ExpandClientTask(task.Ctx, x.clientMeta, client, task) {
				if clientTaskContext.resultChannel == nil {
					clientTaskContext.resultChannel = make(chan any, 1000)
				}
				// You can omit the task field, will use default task's clone
				if clientTaskContext.Task == nil {
					clientTaskContext.Task = task.Clone()
				}
				clientTaskContextSlice = append(clientTaskContextSlice, clientTaskContext)
			}
		} else {
			clientTaskContextSlice = append(clientTaskContextSlice, &ClientTaskContext{
				Client:        client,
				Task:          task.Clone(),
				resultChannel: make(chan any, 1000),
			})
		}
	}
	x.clientMeta.DebugF("taskId = %s, client task context create done, task count = %d", taskId, len(clientTaskContextSlice))
	if len(clientTaskContextSlice) == 0 {
		x.clientMeta.DebugF("taskId = %s, client task count equal zero, so ignored")
		return
	}

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
			// close all result channel
			for _, clientTaskContext := range clientTaskContextSlice {
				close(clientTaskContext.resultChannel)
			}
			wg.Done()

			x.clientMeta.DebugF("taskId = %s, pull table done", taskId)

		}()

		for index, clientTaskContext := range clientTaskContextSlice {
			clientBegin := time.Now()
			x.clientMeta.DebugF("taskId = %s, clientIndex = %d, begin execution pull table...", taskId, index)
			d := task.Table.DataSource.Pull(context.Background(), x.clientMeta, clientTaskContext.Client, clientTaskContext.Task, clientTaskContext.resultChannel)

			clientCost := time.Now().Sub(clientBegin)
			x.clientMeta.DebugF("taskId = %s, clientIndex = %d, execution pull table done, cost = %s", taskId, index, clientCost.String())

			// send diagnostics if not ignore error
			if x.errorsHandlerMeta.IsIgnore(IgnoredErrorOnPullTable) {
				continue
			} else if d != nil {
				task.DiagnosticsChannel <- d
			}
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

		// evert client start one result channel consumer
		for _, clientTaskContext := range clientTaskContextSlice {
			localClientTaskContext := clientTaskContext
			wg.Add(1)
			go func() {
				defer wg.Done()

				for result := range localClientTaskContext.resultChannel {

					// drop nil result
					if reflect_util.IsNil(result) {
						x.clientMeta.DebugF("taskId = %s, return nil result, ignored it", taskId)
						continue
					}

					// run task result handler
					rows, d := x.execResultHandlerWithRecover(task.Ctx, x.clientMeta, localClientTaskContext.Client, localClientTaskContext.Task, result)
					if d != nil && d.HasError() {
						if !isIgnorePullTableError {
							task.DiagnosticsChannel <- d
						}
					} else {
						task.DiagnosticsChannel <- d
					}
					if rows == nil {
						x.clientMeta.DebugF("taskId = %s, task result handler return nil rows", taskId)
						continue
					}

					// The current table parsed to the result of matrix transformation, and divided into a number of only one row of matrices
					for _, row := range rows.SplitRowByRow() {
						// Start a data pull task for each child table
						for _, subTable := range task.Table.SubTables {
							subTask := DataSourcePullTask{

								TaskId: id_util.RandomId(),
								Ctx:    localClientTaskContext.Task.Ctx,

								ParentTask:      localClientTaskContext.Task,
								ParentTable:     localClientTaskContext.Task.Table,
								ParentRow:       row,
								ParentRawResult: result,

								Table:              subTable,
								ResultHandler:      localClientTaskContext.Task.ResultHandler,
								TaskDoneCallback:   localClientTaskContext.Task.TaskDoneCallback,
								DiagnosticsChannel: localClientTaskContext.Task.DiagnosticsChannel,
							}
							x.clientMeta.DebugF("taskId = %s, start subTaskId = %s, parent row = %s, parent raw result = %s", subTask.TaskId, row, result)
							x.execTask(&subTask, localClientTaskContext.Client, false)
						}
					}
				}
			}()
		}
	}()

	// Waiting for the two of you to finish
	wg.Wait()

	taskCost := time.Now().Sub(taskBegin)
	x.clientMeta.DebugF("taskId = %s, execution done, cost = %s", taskCost.String())
}

// Perform the task completion callback while capturing Panic
func (x *DataSourceExecutor) execResultHandlerWithRecover(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, result any) (rows *Rows, diagnostics *Diagnostics) {

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

	rows, diagnostics = task.ResultHandler(ctx, x.clientMeta, client, task, result)
	return
}
