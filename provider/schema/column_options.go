package schema

// ColumnOptions You can customize the options you configure when creating columns
type ColumnOptions struct {

	// Whether the value of this column is unique
	Unique *bool

	// Whether this column is a not-null entry
	NotNull *bool
}

func (x *ColumnOptions) IsUniq() bool {
	return x.Unique != nil && *x.Unique
}

func (x *ColumnOptions) IsNotNull() bool {
	return x.NotNull != nil && *x.NotNull
}
