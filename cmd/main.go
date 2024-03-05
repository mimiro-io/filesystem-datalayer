package main

import (
	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/filesystem-datalayer"
	"os"
)

func main() {
	// either pass in command argument or set DATALAYER_CONFIG_PATH environment variable.
	// if nothing is set, the ServiceRunner defaults to ./config
	configFolderLocation := ""
	args := os.Args[1:]
	if len(args) >= 1 {
		configFolderLocation = args[0]
	}
	common.NewServiceRunner(layer.NewFileSystemDataLayer).WithConfigLocation(configFolderLocation).StartAndWait()
}
