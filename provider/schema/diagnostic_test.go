package schema

import "testing"

func TestDiagnostics_Add(t *testing.T) {
	diagnostics := NewDiagnostics()

	// add nil pointer
	var d2 Diagnostic
	diagnostics.Add(d2)

	// add nil
	diagnostics.Add(nil)
}
