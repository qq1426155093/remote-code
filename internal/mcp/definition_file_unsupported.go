//go:build !linux

package mcpserver

import "fmt"

func readDefinitionFile(name string) ([]byte, physicalFile, error) {
	return nil, physicalFile{}, fmt.Errorf("open MCP definition %q: secure definition loading is not supported on this platform", name)
}
