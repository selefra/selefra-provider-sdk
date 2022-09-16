package schema

import "testing"

func TestRows_String(t *testing.T) {

	rows := NewRows("foo", "bar")
	rows.AppendRowValues([]any{"1", "2"})
	rows.AppendRowValues([]any{"3", "4"})
	t.Log(rows.String())

}
