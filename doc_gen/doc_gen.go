package doc_gen

import (
	"fmt"
	"github.com/selefra/selefra-provider-sdk/provider"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-utils/pkg/string_builder"
	"os"
	"strings"
)

// ProviderDocumentGenerator auto generate provider and it's table documentation
type ProviderDocumentGenerator struct {
	provider        *provider.Provider
	outputDirectory string
}

func New(provider *provider.Provider, outputDirectory string) *ProviderDocumentGenerator {
	return &ProviderDocumentGenerator{
		provider:        provider,
		outputDirectory: outputDirectory,
	}
}

func (x *ProviderDocumentGenerator) Run() error {

	// table
	tableNameSlice := make([]string, 0)
	for _, table := range x.provider.TableList {
		tableNameSlice = append(tableNameSlice, x.genTableDoc(table)...)
	}

	sb := string_builder.New()

	// Provider
	x.genProviderInfo(sb, x.provider)

	// table
	x.genTableList(sb, tableNameSlice)

	return os.WriteFile(x.genProviderDocumentFileName(x.provider.Name), []byte(sb.String()), 0777)
}

func (x *ProviderDocumentGenerator) genProviderInfo(sb *string_builder.StringBuilder, provider *provider.Provider) {

	sb.AppendString("# Provider: ").AppendString(provider.Name).AppendString("\n\n")

	sb.AppendString("## Latest Version \n\n").
		AppendString("```\n").
		AppendString(provider.Version).AppendString("\n").
		AppendString("```\n")

	sb.AppendString("## Description \n\n").
		AppendString(provider.Description).AppendString("\n")

	sb.AppendString("# Install \n\n").
		AppendString("```\n").
		AppendString("selefre  provider install ").AppendString(provider.Name).AppendString("\n").
		AppendString("```\n")

	sb.AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genTableList(sb *string_builder.StringBuilder, tableNameSlice []string) {
	sb.AppendString("## Tables \n\n")
	for _, tableName := range tableNameSlice {
		sb.AppendString(fmt.Sprintf("- [%s](%s)\n", tableName, tableName+".md"))
	}
	sb.AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genProviderDocumentFileName(providerName string) string {
	return x.outputDirectory + "/" + providerName + ".md"
}

// ------------------------------------------------- ------------------------------------------------------------------------

func (x *ProviderDocumentGenerator) genTableDoc(table *schema.Table) []string {
	sb := string_builder.New()

	// title
	sb.AppendString("# Table: ").AppendString(table.TableName).AppendString("\n\n")

	if table.Options != nil {

		// pk
		if len(table.Options.PrimaryKeys) != 0 {
			x.genPrimaryKeys(sb, table.Options.PrimaryKeys)
		}

		// index
		if len(table.Options.Indexes) != 0 {
			x.genIndexes(sb, table.TableName, table.Options.Indexes)
		}

		// fk
		if len(table.Options.ForeignKeys) != 0 {
			x.genForeignKeys(sb, table.TableName, table.Options.ForeignKeys)
		}

	}

	// schema
	if len(table.Columns) != 0 {
		x.genColumns(sb, table, table.Columns)
	}

	tableFileName := x.genTableDocumentFileName(table.TableName)
	_ = os.WriteFile(tableFileName, []byte(sb.String()), 0777)

	tableNameSlice := make([]string, 0)
	tableNameSlice = append(tableNameSlice, table.TableName)
	if len(table.SubTables) != 0 {
		for _, subTable := range table.SubTables {
			tableNameSlice = append(tableNameSlice, x.genTableDoc(subTable)...)
		}
	}
	return tableNameSlice
}

func (x *ProviderDocumentGenerator) genPrimaryKeys(sb *string_builder.StringBuilder, primaryKeys []string) {
	sb.AppendString("## Primary Keys \n\n").
		AppendString("```\n").
		AppendString(strings.Join(primaryKeys, ", ")).AppendString("\n").
		AppendString("```\n").
		AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genIndexes(sb *string_builder.StringBuilder, tableName string, indexes []*schema.TableIndex) {

	sb.AppendString("## Indexes \n\n")

	sb.AppendString("|  Index Name   |  Columns  | Uniq | Description | \n")
	sb.AppendString("|  ----  | ----  | ----  | ---- | \n")

	for _, index := range indexes {

		uniq := "X"
		if index.IsUniq != nil && *index.IsUniq {
			uniq = "√"
		}

		sb.AppendString(fmt.Sprintf("| %s | %s | %s | %s | \n", index.GetName(tableName), strings.Join(index.ColumnNames, ", "), uniq, index.Description))
	}

	sb.AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genForeignKeys(sb *string_builder.StringBuilder, tableName string, foreignKeys []*schema.TableForeignKey) {

	sb.AppendString("## Foreign Keys \n\n")

	sb.AppendString("|  FK Name   |  Self Columns  | Foreign Table | Foreign Columns | Description | \n")
	sb.AppendString("|  ----  | ----  | ----  | ---- | ---- | \n")

	for _, fk := range foreignKeys {
		sb.AppendString(fmt.Sprintf("| %s | %s | %s | %s | %s | \n", fk.GetName(tableName), strings.Join(fk.SelfColumns, ", "),
			fmt.Sprintf("[%s](%s)", fk.ForeignTableName, fk.ForeignTableName+".md"),
			strings.Join(fk.ForeignColumns, ", "), fk.Description))
	}

	sb.AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genColumns(sb *string_builder.StringBuilder, table *schema.Table, columns []*schema.Column) {

	sb.AppendString("## Columns \n\n")

	sb.AppendString("|  Column Name   |  Data Type  | Uniq | Nullable | Description | \n")
	sb.AppendString("|  ----  | ----  | ----  | ----  | ---- | \n")

	for _, column := range columns {

		nullable := "√"
		if column.Options.IsNotNull() {
			nullable = "X"
		}

		uniq := "X"
		if column.Options.IsUniq() || (len(table.GetPrimaryKeys()) == 1 && table.GetPrimaryKeys()[0] == column.ColumnName) {
			uniq = "√"
		}

		sb.AppendString(fmt.Sprintf("| %s | %s | %s | %s | %s | \n", column.ColumnName, column.Type.String(), uniq, nullable, column.Description))
	}

	sb.AppendString("\n\n")
}

func (x *ProviderDocumentGenerator) genTableDocumentFileName(tableName string) string {
	return x.outputDirectory + "/" + tableName + ".md"
}

// ------------------------------------------------- ------------------------------------------------------------------------
