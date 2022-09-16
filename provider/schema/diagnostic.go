package schema

import (
	"fmt"
	"github.com/selefra/selefra-utils/pkg/if_expression"
	"github.com/selefra/selefra-utils/pkg/string_util"
)

// ------------------------------------------------- level -------------------------------------------------------------

type DiagnosticLevel int

const (
	DiagnosisLevelTrace DiagnosticLevel = iota
	DiagnosisLevelDebug
	DiagnosisLevelInfo
	DiagnosisLevelWarn
	DiagnosisLevelError
	DiagnosisLevelFatal
)

func (x DiagnosticLevel) String() string {
	switch x {
	case DiagnosisLevelTrace:
		return "trace"
	case DiagnosisLevelDebug:
		return "debug"
	case DiagnosisLevelInfo:
		return "info"
	case DiagnosisLevelWarn:
		return "warn"
	case DiagnosisLevelError:
		return "error"
	case DiagnosisLevelFatal:
		return "fatal"
	default:
		return "unknown"
	}
}

// ------------------------------------------------- -------------------------------------------------------------------

type Diagnostic struct {
	level   DiagnosticLevel
	content string
}

func NewDiagnostic(level DiagnosticLevel, content string) *Diagnostic {
	return &Diagnostic{
		level:   level,
		content: content,
	}
}

func NewInfoDiagnostic(content string) *Diagnostic {
	return NewDiagnostic(DiagnosisLevelInfo, content)
}

func NewWarnDiagnostic(content string) *Diagnostic {
	return NewDiagnostic(DiagnosisLevelWarn, content)
}

func NewErrorDiagnostic(content string) *Diagnostic {
	return NewDiagnostic(DiagnosisLevelError, content)
}

func NewFatalDiagnostic(content string) *Diagnostic {
	return NewDiagnostic(DiagnosisLevelFatal, content)
}

func (x *Diagnostic) Level() DiagnosticLevel {
	return x.level
}

func (x *Diagnostic) Content() string {
	return x.content
}

// ------------------------------------------------- -------------------------------------------------------------------

// Diagnostics Represents a series of diagnostic information
type Diagnostics struct {

	// Check whether the collected diagnosis information contains ERROR or later diagnosis information
	hasError bool

	// Multiple diagnoses, and there's an order between them
	diagnostics []*Diagnostic
}

func NewDiagnostics() *Diagnostics {
	return &Diagnostics{
		diagnostics: make([]*Diagnostic, 0),
	}
}

// Add d type *Diagnostic or *Diagnostics or error
func (x *Diagnostics) Add(d any) *Diagnostics {
	if d == nil {
		return x
	}
	switch v := d.(type) {
	case *Diagnostic:
		return x.AddDiagnostic(v)
	case Diagnostic:
		return x.AddDiagnostic(&v)
	case *Diagnostics:
		return x.AddDiagnostics(v)
	case Diagnostics:
		return x.AddDiagnostics(&v)
	case error:
		return x.AddError(v)
	default:
		panic("Diagnostics add type error")
	}
}

func (x *Diagnostics) AddInfo(format string, args ...any) *Diagnostics {
	return x._append(NewInfoDiagnostic(fmt.Sprintf(format, args...)))
}

func (x *Diagnostics) AddWarn(format string, args ...any) *Diagnostics {
	return x._append(NewWarnDiagnostic(fmt.Sprintf(format, args...)))
}

func (x *Diagnostics) AddErrorMsg(format string, args ...any) *Diagnostics {
	return x._append(NewErrorDiagnostic(fmt.Sprintf(format, args...)))
}

func NewDiagnosticsAddErrorMsg(format string, args ...any) *Diagnostics {
	return NewDiagnostics().AddErrorMsg(format, args...)
}

func (x *Diagnostics) AddError(err error) *Diagnostics {
	if err == nil {
		return x
	}
	return x._append(NewErrorDiagnostic(err.Error()))
}

func (x *Diagnostics) AddFatal(format string, args ...any) *Diagnostics {
	return x._append(NewFatalDiagnostic(fmt.Sprintf(format, args...)))
}

func (x *Diagnostics) AddDiagnostics(diagnostics *Diagnostics) *Diagnostics {
	if diagnostics != nil {
		for _, diagnostic := range diagnostics.GetDiagnosticSlice() {
			x._append(diagnostic)
		}
	}
	return x
}

func (x *Diagnostics) AddDiagnostic(diagnostic *Diagnostic) *Diagnostics {
	if diagnostic != nil {
		x._append(diagnostic)
	}
	return x
}

func (x *Diagnostics) GetDiagnosticSlice() []*Diagnostic {
	return x.diagnostics
}

func (x *Diagnostics) Size() int {
	return len(x.diagnostics)
}

func (x *Diagnostics) IsEmpty() bool {
	return x.Size() == 0
}

func (x *Diagnostics) HasError() bool {
	return x.hasError
}

func (x *Diagnostics) ToString() string {
	builder := string_util.NewStringBuilder()
	for index, diagnostic := range x.diagnostics {
		builder.WriteString(fmt.Sprintf("[ %s ] ", diagnostic.Level().String())).WriteString(diagnostic.Content())
		if index < len(x.diagnostics)-1 {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

// All additional diagnostic information must be updated using this method and cannot be added directly to the data
func (x *Diagnostics) _append(diagnostic *Diagnostic) *Diagnostics {
	if diagnostic.Level() == DiagnosisLevelError || diagnostic.Level() == DiagnosisLevelFatal {
		x.hasError = true
	}
	x.diagnostics = append(x.diagnostics, diagnostic)
	return x
}

// ------------------------------------------------- helps function ----------------------------------------------------

// AddErrorPullTable There was an error in pull table
func (x *Diagnostics) AddErrorPullTable(table *Table, err error) *Diagnostics {
	if err == nil {
		return x
	}
	return x._append(NewErrorDiagnostic(fmt.Sprintf("pull table %s error: %s", if_expression.ReturnString(table != nil, table.TableName, ""), err.Error())))
}

func NewDiagnosticsErrorPullTable(table *Table, err error) *Diagnostics {
	return NewDiagnostics().AddErrorPullTable(table, err)
}

func (x *Diagnostics) AddErrorMsgPullTable(table *Table, format string, args ...any) *Diagnostics {
	return x._append(NewErrorDiagnostic(fmt.Sprintf("pull table %s error: %s", table.TableName, fmt.Sprintf(format, args...))))
}

func NewDiagnosticsErrorMsgPullTable(table *Table, format string, args ...any) *Diagnostics {
	return NewDiagnostics().AddErrorMsgPullTable(table, format, args...)
}

// AddErrorColumnValueExtractor There was an error in extract column value
func (x *Diagnostics) AddErrorColumnValueExtractor(table *Table, column *Column, err error) *Diagnostics {
	if err == nil {
		return x
	}
	return x._append(NewErrorDiagnostic(fmt.Sprintf("pull table %s column %s extract value error: %s",
		if_expression.ReturnString(table != nil, table.TableName, ""),
		if_expression.ReturnString(column != nil, column.ColumnName, ""),
		err.Error())))
}

func NewDiagnosticsErrorColumnValueExtractor(table *Table, column *Column, err error) *Diagnostics {
	return NewDiagnostics().AddErrorColumnValueExtractor(table, column, err)
}

func (x *Diagnostics) AddErrorMsgColumnValueExtractor(table *Table, column *Column, format string, args ...any) *Diagnostics {
	return x._append(NewErrorDiagnostic(fmt.Sprintf("pull table %s column %s extract value error: %s", table.TableName, column.ColumnName, fmt.Sprintf(format, args...))))
}

func NewDiagnosticsErrorMsgColumnValueExtractor(table *Table, column *Column, format string, args ...any) *Diagnostics {
	return NewDiagnostics().AddErrorMsgColumnValueExtractor(table, column, format, args...)
}

// ---------------------------------------------------------------------------------------------------------------------
