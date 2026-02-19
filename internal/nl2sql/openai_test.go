package nl2sql

import "testing"

func TestStripMarkdownSQL(t *testing.T) {
	got := stripMarkdownSQL("```sql\nSELECT 1;\n```")
	if got != "SELECT 1;" {
		t.Fatalf("stripMarkdownSQL() = %q", got)
	}
}
