package layer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/go-uuid"
	layer "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/common-datalayer/encoder"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type FileSystemDataLayer struct {
	config   *layer.Config
	logger   layer.Logger
	metrics  layer.Metrics
	datasets map[string]*FileSystemDataset
}

func NewFileSystemDataLayer(conf *layer.Config, logger layer.Logger, metrics layer.Metrics) (layer.DataLayerService, error) {
	datalayer := &FileSystemDataLayer{config: conf, logger: logger, metrics: metrics}

	err := datalayer.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}

	return datalayer, nil
}

func (dl *FileSystemDataLayer) Stop(ctx context.Context) error {
	// noop
	return nil
}

func (dl *FileSystemDataLayer) UpdateConfiguration(config *layer.Config) layer.LayerError {
	dl.config = config
	dl.datasets = make(map[string]*FileSystemDataset)

	var err error
	for _, dataset := range config.DatasetDefinitions {
		dl.datasets[dataset.DatasetName], err =
			NewFileSystemDataset(dataset.DatasetName, config.NativeSystemConfig["path"].(string), dataset, dl.logger)
		if err != nil {
			return layer.Err(fmt.Errorf("could not create dataset %s because %s", dataset.DatasetName, err.Error()), layer.LayerErrorInternal)
		}
	}

	return nil
}

func (dl *FileSystemDataLayer) Dataset(dataset string) (layer.Dataset, layer.LayerError) {
	ds, ok := dl.datasets[dataset]
	if !ok {
		return nil, layer.Err(fmt.Errorf("dataset %s not found", dataset), layer.LayerErrorBadParameter)
	}

	return ds, nil
}

func (dl *FileSystemDataLayer) DatasetDescriptions() []*layer.DatasetDescription {
	var datasetDescriptions []*layer.DatasetDescription

	// iterate over the datasest config and create one for each
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &layer.DatasetDescription{Name: key})
	}

	return datasetDescriptions
}

func NewFileSystemDatasetConfig(datasetName string, path string, encoding string, sourceConfig map[string]any) (*FileSystemDatasetConfig, error) {
	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}

	config := &FileSystemDatasetConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	// check if readpath is absolute
	if !filepath.IsAbs(config.ReadPath) {
		config.ReadPath = filepath.Join(path, config.ReadPath)
	}

	// file pattern
	if config.ReadFilePattern == "" {
		// * with the encoding
		config.ReadFilePattern = fmt.Sprintf("*.%s", encoding)
	}

	// check if writepath is absolute
	if !filepath.IsAbs(config.WritePath) {
		config.WritePath = filepath.Join(path, config.WritePath)
	}

	// write full sync file name
	if config.WriteFullSyncFileName == "" {
		// use all but append encoding
		config.WriteFullSyncFileName = fmt.Sprintf("alldata.%s", encoding)
	}

	// write incremental file name
	if config.WriteIncrementalFileName == "" {
		// use dataset name and append encoding
		config.WriteIncrementalFileName = fmt.Sprintf("%s.%s", datasetName, encoding)
	}

	return config, nil
}

type FileSystemDatasetConfig struct {
	Encoding                    string `json:"encoding"`
	ReadPath                    string `json:"read_path"`
	ReadFilePattern             string `json:"read_file_pattern"`
	ReadRecursive               bool   `json:"read_recursive"`
	ReadRecursiveIgnorePattern  string `json:"read_recursive_ignore_pattern"`
	SupportSinceByFileTimestamp bool   `json:"support_since_by_file_timestamp"`
	WritePath                   string `json:"write_path"`
	WriteFullSyncFileName       string `json:"write_full_sync_file"`
	WriteIncrementalFileName    string `json:"write_incremental_file"`
	WriteIncrementalAppend      bool   `json:"write_incremental_append"`
}

func NewFileSystemDataset(name string, path string, datasetDefinition *layer.DatasetDefinition, logger layer.Logger) (*FileSystemDataset, error) {
	sourceConfig := datasetDefinition.SourceConfig

	encoding, ok := sourceConfig["encoding"].(string)
	if !ok {
		return nil, fmt.Errorf("no encoding specified in source config")
	}

	config, err := NewFileSystemDatasetConfig(name, path, encoding, sourceConfig)
	if err != nil {
		return nil, err
	}

	return &FileSystemDataset{name: name,
		config:            config,
		datasetDefinition: datasetDefinition,
		logger:            logger}, nil
}

type FileSystemDataset struct {
	logger            layer.Logger
	name              string                   // dataset name
	datasetDefinition *layer.DatasetDefinition // the dataset definition with mappings etc
	config            *FileSystemDatasetConfig // the dataset config
}

func (f FileSystemDataset) MetaData() map[string]any {
	return make(map[string]any)
}

func (f FileSystemDataset) Name() string {
	return f.name
}

func (f FileSystemDataset) FullSync(ctx context.Context, batchInfo layer.BatchInfo) (layer.DatasetWriter, layer.LayerError) {
	var file *os.File
	var err error
	filePath := filepath.Join(f.config.WritePath, f.config.WriteFullSyncFileName)
	if batchInfo.IsStartBatch {
		file, err = os.Create(filePath)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not create file %s", filePath), layer.LayerErrorInternal)
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not open file %s", filePath), layer.LayerErrorInternal)
		}
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, file, &batchInfo)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := layer.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory}

	return datasetWriter, nil
}

func (f FileSystemDataset) Incremental(ctx context.Context) (layer.DatasetWriter, layer.LayerError) {
	var file *os.File
	var err error

	if f.config.WriteIncrementalAppend {
		filePath := filepath.Join(f.config.WritePath, f.config.WriteIncrementalFileName)

		// if file doesnt exist create it else append
		_, err = os.Stat(filePath)
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not create file %s", filePath), layer.LayerErrorInternal)
			}
		} else {
			file, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not open file for appending %s", filePath), layer.LayerErrorInternal)
			}
		}
	} else {
		id, _ := uuid.GenerateUUID()
		partfileName := fmt.Sprintf("part-%s-%s", id, f.config.WriteIncrementalFileName)
		filePath := filepath.Join(f.config.WritePath, partfileName)
		file, err = os.Create(filePath)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not create file %s", filePath), layer.LayerErrorInternal)
		}
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, file, nil)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := layer.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory}

	return datasetWriter, nil
}

type FileSystemDatasetWriter struct {
	logger  layer.Logger
	enc     encoder.ItemWriter
	factory encoder.ItemFactory
	mapper  *layer.Mapper
}

func (f FileSystemDatasetWriter) Write(entity *egdm.Entity) layer.LayerError {
	item := f.factory.NewItem()
	err := f.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return layer.Err(fmt.Errorf("could not map entity to item because %s", err.Error()), layer.LayerErrorInternal)
	}

	err = f.enc.Write(item)
	if err != nil {
		return layer.Err(fmt.Errorf("could not write item to file because %s", err.Error()), layer.LayerErrorInternal)
	}

	return nil
}

func (f FileSystemDatasetWriter) Close() layer.LayerError {
	err := f.enc.Close()
	if err != nil {
		return layer.Err(fmt.Errorf("could not close file because %s", err.Error()), layer.LayerErrorInternal)
	}
	return nil
}

type FileInfo struct {
	Entry fs.DirEntry
	Path  string
}

// delcare const time format
const timeFormat = "2006-01-02T15:04:05Z07:00"

func (f FileSystemDataset) Changes(since string, limit int, latestOnly bool) (layer.EntityIterator, layer.LayerError) {
	// get root folder
	if _, err := os.Stat(f.config.ReadPath); os.IsNotExist(err) {
		return nil, layer.Err(fmt.Errorf("path %s does not exist", f.config.ReadPath), layer.LayerErrorBadParameter)
	}

	// check if we are recursive and get all folders
	var folders []string
	if f.config.ReadRecursive {
		err := filepath.Walk(f.config.ReadPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// check if the folder should be ignored
			if f.config.ReadRecursiveIgnorePattern != "" {
				matched, err := filepath.Match(f.config.ReadRecursiveIgnorePattern, info.Name())
				if err != nil {
					return err
				}
				if matched {
					// ignore this folder
					return filepath.SkipDir
				}
			}

			if info.IsDir() && info.Name() != f.config.ReadPath {
				folders = append(folders, path)
			}
			return nil
		})

		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not read directory %s", f.config.ReadPath), layer.LayerErrorBadParameter)
		}
	} else {
		folders = append(folders, f.config.ReadPath)
	}

	dataFileInfos := make([]FileInfo, 0)
	allFileInfos := make([]FileInfo, 0)

	// loop over all folders and get all files
	for _, folder := range folders {
		files, err := os.ReadDir(folder)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not read directory %s", folder), layer.LayerErrorBadParameter)
		}

		for _, file := range files {
			fileInfo := FileInfo{Entry: file, Path: folder}
			allFileInfos = append(allFileInfos, fileInfo)
		}
	}

	for _, file := range allFileInfos {
		fileName := file.Entry.Name()
		isMatch, err := filepath.Match(f.config.ReadFilePattern, fileName)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not match file pattern %s", f.config.ReadFilePattern), layer.LayerErrorInternal)
		}

		if isMatch {
			if f.config.SupportSinceByFileTimestamp && since != "" {
				layout := timeFormat
				sinceTime, err := time.Parse(layout, since)
				finfo, err := file.Entry.Info()
				if err != nil {
					return nil, layer.Err(fmt.Errorf("could not get file info for %s", fileName), layer.LayerErrorInternal)
				}
				fileModTime := finfo.ModTime().Truncate(time.Second)
				if sinceTime.Before(fileModTime) {
					dataFileInfos = append(dataFileInfos, file)
				}
			} else {
				dataFileInfos = append(dataFileInfos, file)
			}
		}
	}

	// if since defined using file timestamp, order files based on date, remove files older than since
	if len(dataFileInfos) > 0 {
		sort.Slice(dataFileInfos, func(i, j int) bool {
			f1, _ := dataFileInfos[i].Entry.Info()
			f2, _ := dataFileInfos[j].Entry.Info()
			return f1.ModTime().Before(f2.ModTime())
		})
	}

	mapper := layer.NewMapper(f.logger, nil, f.datasetDefinition.OutgoingMappingConfig)
	iterator := NewFileCollectionEntityIterator(f.datasetDefinition.SourceConfig, dataFileInfos, mapper, "")
	return iterator, nil
}

func (f FileSystemDataset) Entities(from string, limit int) (layer.EntityIterator, layer.LayerError) {
	return nil, layer.Err(fmt.Errorf("operation not supported"), layer.LayerNotSupported)
}

func NewFileCollectionEntityIterator(sourceConfig map[string]any, files []FileInfo, mapper *layer.Mapper, token string) *FileCollectionEntityIterator {
	return &FileCollectionEntityIterator{sourceConfig: sourceConfig, mapper: mapper, token: token, files: files, filesIndex: 0}
}

type FileCollectionEntityIterator struct {
	mapper            *layer.Mapper
	token             string
	files             []FileInfo
	filesIndex        int
	currentItemReader encoder.ItemIterator
	sourceConfig      map[string]any
}

func (f *FileCollectionEntityIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (f *FileCollectionEntityIterator) Next() (*egdm.Entity, layer.LayerError) {
	if f.currentItemReader == nil {
		if f.filesIndex < len(f.files) {
			// initialize the current file entity iterator
			fileInfo := f.files[f.filesIndex]
			file := filepath.Join(fileInfo.Path, fileInfo.Entry.Name())

			// get modified time for the file
			info, err := fileInfo.Entry.Info()
			f.token = info.ModTime().Format(timeFormat)

			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), layer.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
		} else {
			return nil, nil
		}
	}

	// read the next entity from the current file
	item, err := f.currentItemReader.Read()
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not read item from file because %s", err.Error()), layer.LayerErrorInternal)
	}

	if item == nil {
		// close the current file and move to the next
		err := f.currentItemReader.Close()
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not close item reader for file because %s", err.Error()), layer.LayerErrorInternal)
		}
		f.filesIndex++
		if f.filesIndex < len(f.files) {
			fileInfo := f.files[f.filesIndex]
			file := filepath.Join(fileInfo.Path, fileInfo.Entry.Name())

			info, err := fileInfo.Entry.Info()
			f.token = info.ModTime().Format(timeFormat)

			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), layer.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
			item, err = f.currentItemReader.Read()
			if err != nil {
				return nil, layer.Err(fmt.Errorf("could not read item from file because %s", err.Error()), layer.LayerErrorInternal)
			}
		}
	}

	if item == nil {
		return nil, nil
	} else {
		entity := &egdm.Entity{Properties: make(map[string]any)}
		err := f.mapper.MapItemToEntity(item, entity)
		if err != nil {
			return nil, layer.Err(fmt.Errorf("could not map item to entity because %s", err.Error()), layer.LayerErrorInternal)
		}
		return entity, nil
	}
}

func (f *FileCollectionEntityIterator) NewItemReadCloser(filePath string, sourceConfig map[string]any) (encoder.ItemIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not open file %s", filePath), layer.LayerErrorInternal)
	}

	// get encoder for the file
	itemReader, err := encoder.NewItemIterator(sourceConfig, file)
	if err != nil {
		return nil, layer.Err(fmt.Errorf("could not create encoder specified in dataset source config"), layer.LayerErrorBadParameter)
	}

	return itemReader, nil
}

func (f *FileCollectionEntityIterator) Token() (*egdm.Continuation, layer.LayerError) {
	cont := egdm.NewContinuation()
	cont.Token = f.token
	return cont, nil
}

func (f *FileCollectionEntityIterator) Close() layer.LayerError {
	err := f.currentItemReader.Close()
	if err != nil {
		return layer.Err(fmt.Errorf("could not close item reader because %s", err.Error()), layer.LayerErrorInternal)
	}
	return nil
}
