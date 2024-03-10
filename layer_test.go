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

func writeSampleCsv(filename string) error {
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
	err := writeSampleCsv(folderName + "/data.csv")
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

func TestMultiSourceFilesGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	// create some data
	err := writeSampleCsv(folderName + "/data1.csv")
	if err != nil {
		t.Error(err)
	}

	err = writeSampleCsv(folderName + "/data2.csv")
	if err != nil {
		t.Error(err)
	}

	err = writeSampleCsv(folderName + "/data3.txt")
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

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 6 {
		t.Error("Expected 6")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestMultiSourceFilesInFolderHierarchyGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	// create some data
	err := writeSampleCsv(folderName + "/data1.csv")
	if err != nil {
		t.Error(err)
	}

	// create child folder
	childFolderName := folderName + "/child"
	os.MkdirAll(childFolderName, 0777)

	err = writeSampleCsv(childFolderName + "/data2.csv")
	if err != nil {
		t.Error(err)
	}

	childFolderName = folderName + "/child2"
	os.MkdirAll(childFolderName, 0777)

	err = writeSampleCsv(childFolderName + "/data3.csv")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := common_datalayer.NewServiceRunner(NewFileSystemDataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *common_datalayer.Config) error {
		config.NativeSystemConfig["path"] = folderName

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["read_recursive"] = true
				ds.SourceConfig["read_recursive_ignore_pattern"] = "*child2"
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 6 {
		t.Error("Expected 6")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

// create a test for GetChanges with a since filter. write files check timestamp of last and use that.
// expect no rsults
func TestGetChangesWithSinceFilter(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	// create some data
	err := writeSampleCsv(folderName + "/data.csv")
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

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}
	}

	token, err := changes.Token()
	if err != nil {
		t.Error(err)
	}

	changes, err = ds.Changes(token.Token, 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 but got %v", count)
	}

	// write another file
	err = writeSampleCsv(folderName + "/data2.csv")
	if err != nil {
		t.Error(err)
	}

	changes, err = ds.Changes(token.Token, 0, false)
	if err != nil {
		t.Error(err)

	}

	count = 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 3 {
		t.Error("Expected 3")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}

}

// write the same tests as above but for GetEntities
func TestGetEntities(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	folderName := "./test/t-" + guid
	os.MkdirAll(folderName, 0777)

	defer os.RemoveAll(folderName)

	// create some data
	err := writeSampleCsv(folderName + "/data.csv")
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

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	entities, err := ds.Entities("", 0)
	if err != nil {
		t.Error(err)
	}

	entity, err := entities.Next()
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
