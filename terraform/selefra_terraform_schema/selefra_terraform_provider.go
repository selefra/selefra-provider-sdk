package selefra_terraform_schema

import (
	"github.com/selefra/selefra-provider-sdk/provider"
	"github.com/selefra/selefra-provider-sdk/provider/schema"
)

// SelefraTerraformProvider Indicates that the Provider is coming from the Terraform, which sets the relationship
type SelefraTerraformProvider struct {

	// provider's name
	Name string

	// provider's version
	Version string

	// provider's description, let others know what this provider does in general
	Description string

	// A provider may have many tables. Here are the corresponding tables
	ResourceList []*SelefraTerraformResource

	ConfigMeta provider.ConfigMeta

	EventCallback provider.EventCallback

	ClientMeta schema.ClientMeta

	TransformerMeta schema.TransformerMeta

	ErrorsHandlerMeta schema.ErrorsHandlerMeta
}

func (x *SelefraTerraformProvider) ToSelefraProvider(getter TerraformBridgeGetter) (*provider.Provider, *schema.Diagnostics) {
	diagnostics := schema.NewDiagnostics()

	tableSlice, d := x.ToSelefraTableList(getter)
	if diagnostics.AddDiagnostics(d).HasError() {
		return nil, diagnostics
	}

	selefraProvider := &provider.Provider{
		Name:              x.Name,
		Version:           x.Version,
		Description:       x.Description,
		TableList:         tableSlice,
		ConfigMeta:        x.ConfigMeta,
		EventCallback:     x.EventCallback,
		ClientMeta:        x.ClientMeta,
		TransformerMeta:   x.TransformerMeta,
		ErrorsHandlerMeta: x.ErrorsHandlerMeta,
	}

	return selefraProvider, diagnostics
}

func (x *SelefraTerraformProvider) ToSelefraTableList(getter TerraformBridgeGetter) ([]*schema.Table, *schema.Diagnostics) {

	diagnostics := schema.NewDiagnostics()

	// convert resource to table
	tablesMap := make(map[string]*schema.Table)
	tableSlice := make([]*schema.Table, len(x.ResourceList))
	for index, resource := range x.ResourceList {
		table, d := resource.ToTable(getter)
		if diagnostics.AddDiagnostics(d).HasError() {
			continue
		}
		tableSlice[index] = table
		tablesMap[table.TableName] = table
	}
	if diagnostics.HasError() {
		return nil, diagnostics
	}

	// fix table's subtable relation
	for _, resource := range x.ResourceList {
		if len(resource.SubTables) == 0 {
			continue
		}
		table := tablesMap[resource.SelefraTableName]
		if table == nil {
			continue
		}
		for _, subTableName := range resource.SubTables {
			subTable, exists := tablesMap[subTableName]
			if !exists {
				diagnostics.AddErrorMsg("table %s's subtable %s does not exist", table.TableName, subTableName)
				continue
			}
			// Don't try to duplicate, and let the developer find and fix the problem instead of covering it up
			table.SubTables = append(table.SubTables, subTable)
		}
	}

	return tableSlice, diagnostics
}
