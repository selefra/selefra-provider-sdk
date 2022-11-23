package schema

import (
	"context"
	"sync"
)

// DataSourcePullTask Represents a data source pull task
type DataSourcePullTask struct {

	// global uniq task id
	TaskId string

	Ctx context.Context

	ParentTask *DataSourcePullTask

	ParentTable *Table

	ParentRow *Row

	ParentRawResult any

	// Only if the task is successfully executed will the raw result be backfilled here
	// This is so that when a task result is expand, there is a way to access the before expand results if you need to
	NotExpandRawResult any

	Table *Table

	// What happens to the pulled data
	ResultHandler func(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, result any) (*Rows, []any, *Diagnostics)

	// Callback method when the task is completed
	TaskDoneCallback func(ctx context.Context, clientMeta *ClientMeta, task *DataSourcePullTask) *Diagnostics

	// You can pass some messages back at execution time
	DiagnosticsChannel chan *Diagnostics

	// Whether this task is a Root task
	IsRootTask bool
	// The client context required to perform the task
	Client any
	// Is the expansion completed?
	IsExpandDone bool

	itemMap     map[string]any
	itemMapLock sync.RWMutex
}

func (x *DataSourcePullTask) ensureItemMapInit() {

	x.itemMapLock.Lock()
	defer x.itemMapLock.Unlock()

	if x.itemMap == nil {
		x.itemMap = make(map[string]any)
	}
}

func (x *DataSourcePullTask) SetItem(itemName string, itemValue any) {
	x.ensureItemMapInit()

	x.itemMapLock.Lock()
	defer x.itemMapLock.Unlock()

	x.itemMap[itemName] = itemValue
}

func (x *DataSourcePullTask) GetItem(itemName string) any {

	x.ensureItemMapInit()

	x.itemMapLock.RLock()
	defer x.itemMapLock.RUnlock()

	return x.itemMap[itemName]
}

func (x *DataSourcePullTask) LookupItem(itemName string) any {

	x.ensureItemMapInit()

	x.itemMapLock.RLock()
	defer x.itemMapLock.RUnlock()

	// first search myself
	value, exists := x.itemMap[itemName]
	if exists {
		return value
	}

	// then give search to my parent
	if x.ParentTask != nil {
		return x.ParentTask.LookupItem(itemName)
	}

	return nil
}

func (x *DataSourcePullTask) GetStringItem(itemName, defaultValue string) string {

	x.ensureItemMapInit()

	item := x.GetItem(itemName)
	if item == nil {
		return defaultValue
	}

	value, ok := item.(string)
	if !ok {
		return defaultValue
	}

	return value
}

func (x *DataSourcePullTask) LookupStringItem(itemName, defaultValue string) string {

	x.ensureItemMapInit()

	item := x.LookupItem(itemName)
	if item == nil {
		return defaultValue
	}

	value, ok := item.(string)
	if !ok {
		return defaultValue
	}

	return value
}

func (x *DataSourcePullTask) GetIntItem(itemName string, defaultValue int) int {

	x.ensureItemMapInit()

	item := x.GetItem(itemName)
	if item == nil {
		return defaultValue
	}

	value, ok := item.(int)
	if !ok {
		return defaultValue
	}

	return value
}

func (x *DataSourcePullTask) LookupIntItem(itemName string, defaultValue int) int {

	x.ensureItemMapInit()

	item := x.LookupItem(itemName)
	if item == nil {
		return defaultValue
	}

	value, ok := item.(int)
	if !ok {
		return defaultValue
	}

	return value
}

func (x *DataSourcePullTask) ClearItem() {

	x.itemMapLock.RLock()
	defer x.itemMapLock.RUnlock()

	x.itemMap = make(map[string]any)
}

func (x *DataSourcePullTask) Clone() *DataSourcePullTask {

	x.itemMapLock.Lock()
	defer x.itemMapLock.Unlock()
	itemMap := make(map[string]any)
	for key, value := range x.itemMap {
		itemMap[key] = value
	}

	return &DataSourcePullTask{
		TaskId: x.TaskId,
		Ctx:    x.Ctx,

		ParentTask:      x.ParentTask,
		ParentTable:     x.ParentTable,
		ParentRow:       x.ParentRow,
		ParentRawResult: x.ParentRawResult,

		Table:              x.Table,
		NotExpandRawResult: x.NotExpandRawResult,
		ResultHandler:      x.ResultHandler,
		TaskDoneCallback:   x.TaskDoneCallback,
		DiagnosticsChannel: x.DiagnosticsChannel,

		itemMap:     itemMap,
		itemMapLock: sync.RWMutex{},

		IsRootTask:   x.IsRootTask,
		IsExpandDone: x.IsExpandDone,
		Client:       x.Client,
	}
}
