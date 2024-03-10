package layer

import (
	"encoding/csv"
	"github.com/google/uuid"
	common_datalayer "github.com/mimiro-io/common-datalayer"
	"os"
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

func writeSampleCsv(path string) error {
	filename := path + "/data.csv"
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// create csv writer
	writer := csv.NewWriter(file)
	writer.Write([]string{"id", "name", "age", "worksfor"})
	writer.Write([]string{"1", "John", "30", "Mimiro"})
	writer.Write([]string{"2", "Jane", "25", "Mimiro"})
	writer.Write([]string{"3", "Jim", "35", "Mimiro"})
	writer.Flush()
	return writer.Error()
}

func TestGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	// create some data
	err := writeSampleCsv(folderName)
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := common_datalayer.NewServiceRunner(NewFileSystemDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *common_datalayer.Config) error {
		config.NativeSystemConfig["path"] = folderName
		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	// get the service so we can make class and not use the http endpoint
	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err := changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	if entity.ID != "http://data.sample.org/things/1" {
		t.Error("Expected 1")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}
