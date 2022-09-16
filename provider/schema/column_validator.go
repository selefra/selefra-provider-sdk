package schema

import (
	"context"
	"fmt"
	"strings"
)

// Verify the validity of the column
type columnValidator struct {
}

func (x *columnValidator) validate(ctx context.Context, clientMeta *ClientMeta, parentTable *Table, table *Table, column *Column) *Diagnostics {

	diagnostics := NewDiagnostics()

	// Column name related checks, length, whitespace, characters, etc
	if column.ColumnName == "" || strings.TrimSpace(column.ColumnName) == "" {
		diagnostics.AddErrorMsg(x.buildMsg(table, column, "column name can not empty"))
	}

	// Column names cannot contain Spaces
	if strings.Contains(column.ColumnName, " ") {
		diagnostics.AddErrorMsg(x.buildMsg(table, column, "column name can not contains whitespace"))
	}

	// column's length
	if len(column.ColumnName) > 60 {
		diagnostics.AddErrorMsg(x.buildMsg(table, column, "column name length must <=60"))
	}

	// You must assign a type to the column
	if column.Type == ColumnTypeNotAssign {
		diagnostics.AddErrorMsg(x.buildMsg(table, column, "column must assign type"))
	}

	// describe
	//if column.Description == "" {
	//	diagnostics.AddWarn(x.buildMsg(table, column, "it is recommended to add description for column"))
	//}

	// Value extractor
	if column.Extractor != nil {
		// If so, check to see if it's set correctly
		diagnostics.AddDiagnostics(column.Extractor.Validate(ctx, clientMeta, parentTable, table, column))
	}

	return diagnostics
}

func (x *columnValidator) buildMsg(table *Table, column *Column, msg string) string {
	return fmt.Sprintf("table %s column %s validate: %s", table.TableName, column.ColumnName, msg)
}
