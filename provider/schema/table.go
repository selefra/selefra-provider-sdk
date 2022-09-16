package schema

import "context"

// Table A Provider may have many tables, which are the API's persistent representation
type Table struct {

	// Table's name
	TableName string

	// You can provide some description information, which will be included in the automatic document generation
	Description string

	// What are the columns in this table
	Columns []*Column

	// A table can have child tables whose data depends on the current table
	SubTables []*Table

	// Some configuration items of this table
	Options *TableOptions

	// The data source used to provide data for this table
	DataSource DataSource

	// If your table needs to extend the default call method, you can implement this function
	// The default is to use Task once per client
	// But you can call the client multiple times by making multiple copies of the client
	// When a value is returned, it is called once with each of the returned clients
	// The second parameter, task, if it is not returned, uses the original task. If it is returned, it must be the same length as the client and correspond one to one
	ExpandClientTask func(ctx context.Context, clientMeta *ClientMeta, client any, task *DataSourcePullTask) []*ClientTaskContext

	// An increasing number. If the table structure has changed, the version number needs to be changed
	Version uint64

	// The runtime of the table
	runtime TableRuntime
}

func (x *Table) GetPrimaryKeys() []string {
	if x.Options == nil || len(x.Options.PrimaryKeys) == 0 {
		return nil
	}
	return x.Options.PrimaryKeys
}

func (x *Table) GetFullTableName() string {
	if x.GetNamespace() != "" {
		return x.GetNamespace() + "." + x.TableName
	} else {
		return x.TableName
	}
}

func (x *Table) GetNamespace() string {
	return x.runtime.Namespace
}

func (x *Table) Runtime() *TableRuntime {
	return &x.runtime
}
