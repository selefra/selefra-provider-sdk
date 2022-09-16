package schema

type ClientTaskContext struct {

	// client use execution task
	Client any

	// client's task
	Task *DataSourcePullTask

	// collect client execution result, The internal field, the executor will initialize it
	resultChannel chan any
}
