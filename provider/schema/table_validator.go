package schema

import (
	"context"
	"fmt"
)

// Check the validity of the table
type tableValidator struct {
	myTable *Table
}

func (x *tableValidator) validate(ctx context.Context, clientMeta *ClientMeta, parentTable *Table, myTable *Table) *Diagnostics {

	x.myTable = myTable

	diagnostics := NewDiagnostics()

	// table name
	if myTable.TableName == "" {
		diagnostics.AddErrorMsg(x.buildMsg("table name must not be empty"))
	}

	// table description
	//if myTable.Description == "" {
	//	diagnostics.AddErrorMsg(x.buildMsg("it is recommended to add description for table"))
	//}

	// table name uniq
	columnNameSet := make(map[string]struct{}, 0)
	for _, column := range myTable.Columns {

		if _, exists := columnNameSet[column.ColumnName]; exists {
			diagnostics.AddErrorMsg(x.buildMsg("cannot have columns with the same name"))
		}
		columnNameSet[column.ColumnName] = struct{}{}

		// column
		diagnostics.AddDiagnostics(column.Runtime().validator.validate(ctx, clientMeta, parentTable, myTable, column))

	}

	if myTable.Options != nil {

		if myTable.Options.PrimaryKeys != nil {
			for _, columnName := range myTable.Options.PrimaryKeys {
				if !myTable.runtime.ContainsColumnName(columnName) {
					diagnostics.AddErrorMsg(x.buildMsg(fmt.Sprintf("PrimaryKeys: table %s does not contain column %s", myTable.TableName, columnName)))
				}
			}
		}

		if myTable.Options.Indexes != nil {
			for _, tableIndex := range myTable.Options.Indexes {
				for _, columnName := range tableIndex.ColumnNames {
					if !myTable.runtime.ContainsColumnName(columnName) {
						diagnostics.AddErrorMsg(x.buildMsg(fmt.Sprintf("Index: table %s does not contain column %s", myTable.TableName, columnName)))
					}
				}
			}
		}

		// do not validate fk, because can not access provider in here
		//if myTable.Options.ForeignKeys != nil {
		//	// check foreign keys exists
		//	for _, fk := range myTable.Options.ForeignKeys {
		//		diagnostics.Add(x.validateForeignKey(fk))
		//	}
		//}

	}

	if parentTable != nil {
		// do what?
	}

	// sub table recursive check
	for _, subTable := range myTable.SubTables {
		diagnostics.AddDiagnostics(subTable.runtime.validator.validate(ctx, clientMeta, myTable, subTable))
	}

	return diagnostics
}

//func (x *tableValidator) validateForeignKey(fk *TableForeignKey) *Diagnostics {
//
//	// fk table exists
//
//
//	return nil
//}

func (x *tableValidator) buildMsg(msg string) string {
	return fmt.Sprintf("table %s validate error: %s", x.myTable.TableName, msg)
}
