package schema

type ClientTaskContext struct {

	// client use execution task
	Client any

	// client's task
	Task *DataSourcePullTask
}
