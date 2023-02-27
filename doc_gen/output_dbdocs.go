package doc_gen

import (
	"context"
	"errors"
	"fmt"
	"github.com/selefra/selefra-provider-sdk/env"
	"github.com/selefra/selefra-provider-sdk/grpc/shard"
	"github.com/selefra/selefra-provider-sdk/provider"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-provider-sdk/storage/database_storage/postgresql_storage"
	"github.com/selefra/selefra-utils/pkg/pointer"
	"strings"
)

func GeneratorDBDocsWithPostgresqlDSNEnv(provider *provider.Provider) (string, error) {

	if env.GetDatabaseDsn() == "" {
		return "", fmt.Errorf("must config env %s", env.DatabaseDsn)
	}

	postgresqlOptions := postgresql_storage.PostgresqlStorageOptions{ConnectionString: env.GetDatabaseDsn()}
	postgresqlOptionsJsonString, err := postgresqlOptions.ToJsonString()
	if err != nil {
		return "", err
	}
	initRequest := &shard.ProviderInitRequest{
		Storage: &shard.Storage{
			Type:           shard.POSTGRESQL,
			StorageOptions: []byte(postgresqlOptionsJsonString),
		},
		Workspace:      pointer.ToStringPointer("./"),
		ProviderConfig: nil,
	}
	response, err := provider.Init(context.Background(), initRequest)
	if err != nil {
		return "", err
	}
	if response.Diagnostics != nil && response.Diagnostics.HasError() {
		return "", errors.New(response.Diagnostics.String())
	}

	return NewDBDocsGenerator(provider).Run(), nil
}

// DBDocsGenerator Generate dbdocs(https://dbdocs.io/) support DBML (https://www.dbml.org/home/)
type DBDocsGenerator struct {
	provider *provider.Provider
	output   strings.Builder
}

func NewDBDocsGenerator(provider *provider.Provider) *DBDocsGenerator {
	return &DBDocsGenerator{
		output:   strings.Builder{},
		provider: provider,
	}
}

func (x *DBDocsGenerator) Run() string {

	x.GenProjectBlock()

	x.GenTables()

	x.GenFK()

	return x.output.String()
}

// GenProjectBlock
//
// example:
//
//	Project Ecommerce {
//	 database_type: 'PostgreSQL'
//	 Note: '''
//	   # Ecommerce Database
//	   **markdown content here**
//	 '''
//	}
func (x *DBDocsGenerator) GenProjectBlock() {

	// TODO Do more compatibility work
	// project name
	x.output.WriteString(fmt.Sprintf("Project %s {\n", strings.ReplaceAll(x.provider.Name, "-", "_")))

	// Temporarily written dead, after the need to change then modify
	x.output.WriteString(fmt.Sprintf("\tdatabase_type: 'PostgreSQL'\n"))

	// description
	x.output.WriteString("\tNote: '''\n\t")
	x.output.WriteString(x.provider.Description)
	x.output.WriteString("\n")
	x.output.WriteString("\t'''\n")
	x.output.WriteString("}\n")
	x.output.WriteString("\n\n")

}

func (x *DBDocsGenerator) GenTables() {
	for _, table := range x.provider.TableList {
		x.GenTable(table)
	}
}

func (x *DBDocsGenerator) GenTable(table *schema.Table) {

	for _, subTable := range table.SubTables {
		x.GenTable(subTable)
	}

	// table name
	x.output.WriteString(fmt.Sprintf("Table %s {\n", table.TableName))

	// columns
	for _, column := range table.Columns {
		x.output.WriteString("\t")
		x.GenColumn(table, column)
		x.output.WriteString("\n")
	}

	x.output.WriteString("}\n\n")
}

func (x *DBDocsGenerator) GenColumn(table *schema.Table, column *schema.Column) {
	s := strings.Builder{}

	// column name
	s.WriteString(column.ColumnName)

	// column type
	sqlType, diagnostics := postgresql_storage.GetColumnPostgreSQLType(table, column)
	if diagnostics != nil && diagnostics.HasError() {
		panic(diagnostics.String())
	}
	s.WriteString(" ")
	s.WriteString(strings.ReplaceAll(sqlType, "timestamp without time zone", "timestamp"))

	options := make([]string, 0)
	if table.Runtime().IsPrimaryKey(column.ColumnName) {
		options = append(options, "pk")
	}
	if pointer.FromBoolPointer(column.Options.Unique) {
		options = append(options, "unique")
	}
	if pointer.FromBoolPointer(column.Options.NotNull) {
		options = append(options, "not null")
	}
	if len(options) != 0 {
		s.WriteString(fmt.Sprintf(" [%s]", strings.Join(options, ", ")))
	}

	x.output.WriteString(s.String())
}

func (x *DBDocsGenerator) GenFK() {
	toParentTableMap := x.provider.Runtime().MakeToParentTableMap()
	for _, table := range x.provider.TableList {
		x.internalGenFK(toParentTableMap, table)
	}
}

func (x *DBDocsGenerator) internalGenFK(toParentTableMap map[string]*schema.Table, table *schema.Table) {

	for _, subTable := range table.SubTables {
		x.internalGenFK(toParentTableMap, subTable)
	}

	// Displays the declared foreign key on the table structure
	if table.Options != nil {
		for _, fkSchema := range table.Options.ForeignKeys {
			s := fmt.Sprintf("Ref: %s.%s > %s.%s  \n", table.TableName, fkSchema.SelfColumns[0], fkSchema.ForeignTableName, fkSchema.ForeignColumns[0])
			x.output.WriteString(s)
		}
	}

	// The dependencies contained in the extractor
	parentTable, exists := toParentTableMap[table.TableName]
	if exists && parentTable != nil {
		for _, column := range table.Columns {
			// The parent column used in the extractor
			parentPrimaryKeyExtractor, ok := column.Extractor.(*column_value_extractor.ColumnValueExtractorParentColumnValue)
			if ok {
				s := fmt.Sprintf("Ref: %s.%s > %s.%s  \n", table.TableName, column.ColumnName, parentTable.TableName, parentPrimaryKeyExtractor.GetParentTableColumnName())
				x.output.WriteString(s)
			}
		}
	}
}
