package schema

import (
	"github.com/selefra/selefra-utils/pkg/md5_util"
	"strings"
)

// TableOptions When you create a table, you can specify primary keys, foreign keys, indexes, and so on
type TableOptions struct {

	// Primary key: Which columns are the primary keys of the table.
	// Note that this is in order. The primary keys are created in the given order
	PrimaryKeys []string

	// Foreign key: This table can be associated to other tables through foreign keys
	ForeignKeys []*TableForeignKey

	// Indexes: There are some indexes that can be defined in a table. Generally,
	// compound indexes are defined in this place. If an index involves only one column, then it is OK to define on the column
	Indexes []*TableIndex
}

// GenPrimaryKeysName Automatically generate the name of the primary key
func (x *TableOptions) GenPrimaryKeysName(tableName string) string {
	defaultName := "pk_" + tableName + "_" + strings.Join(x.PrimaryKeys, "_")
	if len(defaultName) > 63 {
		md5, err := md5_util.Md5String(defaultName)
		if err != nil {
			// TODO 2022-7-22 18:05:17
		} else {
			defaultName = "fk_" + md5
		}
	}
	return defaultName
}

// -------------------------------------------------------------------------------------------------------------------------

// TableForeignKey Foreign key table
type TableForeignKey struct {

	// Leaving it unset automatically generates a name
	Name string

	// The column name of the current table
	SelfColumns []string

	// The table to associate with
	ForeignTableName string

	// The column of the table to be associated with
	ForeignColumns []string

	Description string
}

func (x *TableForeignKey) GetName(tableName string) string {
	if x.Name == "" {
		defaultName := "fk_" + tableName + "_" + strings.Join(x.SelfColumns, "_") + "_to_" + x.ForeignTableName + "_" + strings.Join(x.ForeignColumns, "_")
		if len(defaultName) > 63 {
			md5, err := md5_util.Md5String(defaultName)
			if err != nil {
				// TODO 2022-7-22 18:05:17
			} else {
				defaultName = "fk_" + md5
			}
		}
		x.Name = defaultName
	}
	return x.Name
}

// -------------------------------------------------------------------------------------------------------------------------

// TableIndex You can create indexes, composite indexes, and the like to speed up queries
type TableIndex struct {

	// index's name
	Name string

	// Index the columns involved
	ColumnNames []string

	// Whether this index is unique
	IsUniq *bool

	// Please briefly explain what this index does
	Description string
}

func (x *TableIndex) GetName(tableName string) string {
	if x.Name == "" {
		defaultName := "idx_" + tableName + "_" + strings.Join(x.ColumnNames, "_")
		if len(defaultName) > 63 {
			md5, err := md5_util.Md5String(defaultName)
			if err != nil {
				// TODO 2022-7-22 18:05:17
			} else {
				defaultName = "fk_" + md5
			}
		}
		x.Name = defaultName
	}
	return x.Name
}
