package layer

import (
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"testing"
)

func TestStartStopFileSystemDataLayer(t *testing.T) {
	configLocation := "./config"
	serviceRunner := common_datalayer.NewServiceRunner(NewFileSystemDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *common_datalayer.Config) error {
		config.NativeSystemConfig["path"] = "/tmp"
		return nil
	})
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}
