package schema

type IgnoredError int

const (

	// IgnoredErrorAll Ignore the following types of errors
	IgnoredErrorAll IgnoredError = iota

	// IgnoredErrorOnPullTable When an error occurs while pulling the table, the error is ignored and the user is not reported, but the log is printed silently
	IgnoredErrorOnPullTable

	// IgnoredErrorOnTransformerRow Ignore errors when converting row
	IgnoredErrorOnTransformerRow

	// IgnoredErrorOnTransformerCell Ignore errors when converting cell
	IgnoredErrorOnTransformerCell

	// IgnoredErrorOnSaveResult If an error is reported when the retrieved data is saved to the table, the system ignores
	// the error and does not print it to the user. Instead, it silently records a log
	IgnoredErrorOnSaveResult
)

type ErrorsHandlerMeta struct {

	// You can configure which types of errors are blocked from users
	IgnoredErrors []IgnoredError

	runtime *ErrorsHandlerMetaRuntime
}

func (x *ErrorsHandlerMeta) IsIgnore(err IgnoredError) bool {
	return x.runtime.IsNeedIgnore(err)
}
