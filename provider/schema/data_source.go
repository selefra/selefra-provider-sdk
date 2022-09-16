package schema

import (
	"context"
	"github.com/selefra/selefra-utils/pkg/channel_util"
	"github.com/songzhibin97/go-ognl"
)

// DataSource Data sources can produce data
type DataSource struct {

	// The pull method is responsible for pulling the data, The pulled data is returned to the resultChannel, one result at a time
	Pull func(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, resultChannel chan<- any) *Diagnostics
}

// DataSourceParentRawFieldSliceValue Gets a slice value from the original return result of the parent table
func DataSourceParentRawFieldSliceValue(structSelector string) DataSource {
	return DataSource{
		Pull: func(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, resultChannel chan<- any) *Diagnostics {
			valueSlice := ognl.Get(task.ParentRawResult, structSelector).Values()
			if len(valueSlice) != 0 {
				channel_util.SendSliceToChannel(valueSlice, resultChannel)
			}
			return nil
		},
	}
}

// DataSourceParentRawFieldValue Gets a value from the original return result of the parent table
func DataSourceParentRawFieldValue(structSelector string) DataSource {
	return DataSource{
		Pull: func(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask, resultChannel chan<- any) *Diagnostics {
			value := ognl.Get(task.ParentRawResult, structSelector).Values()
			if value != nil {
				resultChannel <- value
			}
			return nil
		},
	}
}
