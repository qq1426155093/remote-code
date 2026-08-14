package main

import "testing"

func TestCommandFlags(t *testing.T) {
	flags := make(commandFlags)
	if err := flags.Set("claude=/usr/local/bin/claude"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if got := flags.String(); got != "claude=/usr/local/bin/claude" {
		t.Errorf("String() = %q", got)
	}
	if err := flags.Set("claude=/other"); err == nil {
		t.Error("Set(duplicate) succeeded")
	}
	for _, value := range []string{"", "missing-equals", "=missing-name", "missing-path="} {
		if err := flags.Set(value); err == nil {
			t.Errorf("Set(%q) succeeded", value)
		}
	}
}
