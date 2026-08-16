package cli

import "fmt"

func (r *REPL) controllerInfo(arguments []string) error {
	if len(arguments) != 0 {
		return usageError()
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.GetInfo(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "Controller version: %s\nAPI version: %s\nWorkspace: %s\nMax upload bytes: %d\nResumable upload: %t\nResumable download: %t\nPreferred transfer chunk bytes: %d\nMax processes: %d\nProcess template count: %d\n",
		info.GetControllerVersion(), info.GetApiVersion(), info.GetWorkspaceName(), info.GetMaxUploadBytes(),
		info.GetFileTransfers().GetResumableUpload(), info.GetFileTransfers().GetResumableDownload(), info.GetFileTransfers().GetPreferredChunkBytes(),
		info.GetMaxProcesses(), info.GetProcessTemplateCount())
	return nil
}
