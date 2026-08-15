//go:build !linux

package process

import "fmt"

func readProcessTemplateDefinitionFile(name string) ([]byte, processTemplatePhysicalFile, error) {
	return nil, processTemplatePhysicalFile{}, fmt.Errorf("open process template definition %q: secure definition loading is not supported on this platform", name)
}
