package doc_gen

import (
	"github.com/selefra/selefra-provider-sdk/provider"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-utils/pkg/pointer"
	"testing"
)

func getTestProvider() *provider.Provider {
	testProvider := &provider.Provider{
		Name:        "test-provider",
		Version:     "v0.0.1",
		Description: "test provider",
		TableList: []*schema.Table{
			&schema.Table{
				TableName: "user_test",
				Options: &schema.TableOptions{
					PrimaryKeys: []string{
						"name",
					},
					Indexes: []*schema.TableIndex{
						{
							Name:        "index",
							ColumnNames: []string{"passwd"},
							Description: "for fast query",
						},
					},
				},
				Columns: []*schema.Column{
					{
						ColumnName:  "name",
						Type:        schema.ColumnTypeString,
						Description: "",
						Extractor:   column_value_extractor.StructSelector(".Name"),
						Options: schema.ColumnOptions{
							NotNull: pointer.TruePointer(),
							Unique:  pointer.TruePointer(),
						},
					},
					{
						ColumnName:  "age",
						Type:        schema.ColumnTypeBigInt,
						Description: "",
						Extractor:   column_value_extractor.StructSelector(".Age"),
						Options: schema.ColumnOptions{
							NotNull: pointer.TruePointer(),
						},
					},
					{
						ColumnName:  "passwd",
						Type:        schema.ColumnTypeString,
						Description: "",
						Extractor:   column_value_extractor.StructSelector(".Passwd"),
						Options: schema.ColumnOptions{
							NotNull: pointer.TruePointer(),
						},
					},
					{
						ColumnName:  "dog",
						Type:        schema.ColumnTypeString,
						Description: "",
						Extractor:   column_value_extractor.StructSelector(".Dog"),
						Options: schema.ColumnOptions{
							NotNull: pointer.TruePointer(),
						},
					},
					{
						ColumnName:  "test_id",
						Type:        schema.ColumnTypeString,
						Description: "",
						Extractor:   column_value_extractor.ColumnsValueMd5("name", "passwd"),
						Options: schema.ColumnOptions{
							NotNull: pointer.TruePointer(),
							Unique:  pointer.TruePointer(),
						},
					},
				},
				SubTables: []*schema.Table{
					&schema.Table{
						TableName: "user_dog",
						Options: &schema.TableOptions{
							ForeignKeys: []*schema.TableForeignKey{
								{
									SelfColumns:      []string{"master"},
									ForeignTableName: "user_test",
									ForeignColumns:   []string{"name"},
								},
							},
						},
						Columns: []*schema.Column{
							{
								ColumnName:  "name",
								Type:        schema.ColumnTypeString,
								Description: "",
								Extractor:   column_value_extractor.StructSelector(".Name"),
								Options: schema.ColumnOptions{
									NotNull: pointer.TruePointer(),
									Unique:  pointer.TruePointer(),
								},
							},
							{
								ColumnName:  "master",
								Type:        schema.ColumnTypeString,
								Description: "",
								Extractor:   column_value_extractor.StructSelector(".Master"),
								Options: schema.ColumnOptions{
									NotNull: pointer.TruePointer(),
								},
							},
							{
								ColumnName:  "age",
								Type:        schema.ColumnTypeInt,
								Description: "",
								Extractor:   column_value_extractor.StructSelector(".Age"),
								Options: schema.ColumnOptions{
									NotNull: pointer.TruePointer(),
								},
							},
						},
					},
				},
			},
		},
	}
	return testProvider
}

func TestProviderDocumentGenerator_Run(t *testing.T) {

	myProvider := getTestProvider()
	New(myProvider, "./docs/").Run()

}
