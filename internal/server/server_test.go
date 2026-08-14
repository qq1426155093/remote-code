package server

import "testing"

func TestIsLoopbackAddress(t *testing.T) {
	tests := map[string]bool{
		"127.0.0.1:9443": true,
		"[::1]:9443":     true,
		"localhost:9443": true,
		"0.0.0.0:9443":   false,
		":9443":          false,
		"invalid":        false,
	}
	for address, want := range tests {
		if got := isLoopbackAddress(address); got != want {
			t.Errorf("isLoopbackAddress(%q) = %v, want %v", address, got, want)
		}
	}
}

func TestNewRejectsInsecureRemoteListener(t *testing.T) {
	_, err := New(Config{ListenAddress: "0.0.0.0:0", Workspace: t.TempDir()})
	if err == nil {
		t.Fatal("New() accepted insecure non-loopback listener")
	}
}
