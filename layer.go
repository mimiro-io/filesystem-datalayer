package layer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	cdl "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/common-datalayer/encoder"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

type FileSystemDataLayer struct {
	config   *cdl.Config
	logger   cdl.Logger
	metrics  cdl.Metrics
	datasets map[string]*FileSystemDataset
}

func NewFileSystemDataLayer(conf *cdl.Config, logger cdl.Logger, metrics cdl.Metrics) (cdl.DataLayerService, error) {
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

func (dl *FileSystemDataLayer) UpdateConfiguration(config *cdl.Config) cdl.LayerError {
	dl.config = config
	dl.datasets = make(map[string]*FileSystemDataset)

	var err error
	for _, dataset := range config.DatasetDefinitions {
		dl.datasets[dataset.DatasetName], err =
			NewFileSystemDataset(dataset.DatasetName, config.NativeSystemConfig["path"].(string), dataset, dl.logger)
		if err != nil {
			return cdl.Err(fmt.Errorf("could not create dataset %s because %s", dataset.DatasetName, err.Error()), cdl.LayerErrorInternal)
		}
	}

	return nil
}

func (dl *FileSystemDataLayer) Dataset(dataset string) (cdl.Dataset, cdl.LayerError) {
	ds, ok := dl.datasets[dataset]
	if !ok {
		return nil, cdl.Err(fmt.Errorf("dataset %s not found", dataset), cdl.LayerErrorBadParameter)
	}

	return ds, nil
}

func (dl *FileSystemDataLayer) DatasetDescriptions() []*cdl.DatasetDescription {
	var datasetDescriptions []*cdl.DatasetDescription

	// iterate over the datasest testconfig and create one for each
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &cdl.DatasetDescription{Name: key})
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

func NewFileSystemDataset(name string, path string, datasetDefinition *cdl.DatasetDefinition, logger cdl.Logger) (*FileSystemDataset, error) {
	sourceConfig := datasetDefinition.SourceConfig

	encoding, ok := sourceConfig["encoding"].(string)
	if !ok {
		return nil, fmt.Errorf("no encoding specified in source testconfig")
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
	logger            cdl.Logger
	name              string                   // dataset name
	datasetDefinition *cdl.DatasetDefinition   // the dataset definition with mappings etc
	config            *FileSystemDatasetConfig // the dataset config
}

func (f FileSystemDataset) MetaData() map[string]any {
	return make(map[string]any)
}

func (f FileSystemDataset) Name() string {
	return f.name
}

func (f FileSystemDataset) FullSync(ctx context.Context, batchInfo cdl.BatchInfo) (cdl.DatasetWriter, cdl.LayerError) {
	var file *os.File
	var err error
	filePath := filepath.Join(f.config.WritePath, f.config.WriteFullSyncFileName)
	tmpFilePath := filePath + "." + batchInfo.SyncId + ".tmp"
	if batchInfo.IsStartBatch {
		file, err = os.Create(tmpFilePath)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not create file %s", tmpFilePath), cdl.LayerErrorInternal)
		}
	} else {
		file, err = os.OpenFile(tmpFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not open file %s", tmpFilePath), cdl.LayerErrorInternal)
		}
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, f.logger, file, &batchInfo)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := cdl.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory, tmpFullSyncPath: tmpFilePath, fullSyncFilePath: filePath, closeFullSync: batchInfo.IsLastBatch}

	return datasetWriter, nil
}

func (f FileSystemDataset) Incremental(ctx context.Context) (cdl.DatasetWriter, cdl.LayerError) {
	var file *os.File
	var err error

	if f.config.WriteIncrementalAppend {
		filePath := filepath.Join(f.config.WritePath, f.config.WriteIncrementalFileName)

		// if file doesnt exist create it else append
		_, err = os.Stat(filePath)
		if os.IsNotExist(err) {
			file, err = os.Create(filePath)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not create file %s", filePath), cdl.LayerErrorInternal)
			}
		} else {
			file, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not open file for appending %s", filePath), cdl.LayerErrorInternal)
			}
		}
	} else {
		id := uuid.New().String()
		partfileName := fmt.Sprintf("part-%s-%s", id, f.config.WriteIncrementalFileName)
		filePath := filepath.Join(f.config.WritePath, partfileName)
		file, err = os.Create(filePath)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not create file %s", filePath), cdl.LayerErrorInternal)
		}
	}

	enc, err := encoder.NewItemWriter(f.datasetDefinition.SourceConfig, f.logger, file, nil)
	factory, err := encoder.NewItemFactory(f.datasetDefinition.SourceConfig)
	mapper := cdl.NewMapper(f.logger, f.datasetDefinition.IncomingMappingConfig, f.datasetDefinition.OutgoingMappingConfig)
	datasetWriter := &FileSystemDatasetWriter{logger: f.logger, enc: enc, mapper: mapper, factory: factory}

	return datasetWriter, nil
}

type FileSystemDatasetWriter struct {
	logger           cdl.Logger
	enc              encoder.ItemWriter
	factory          encoder.ItemFactory
	mapper           *cdl.Mapper
	tmpFullSyncPath  string
	fullSyncFilePath string
	closeFullSync    bool
}

func (f FileSystemDatasetWriter) Write(entity *egdm.Entity) cdl.LayerError {
	item := f.factory.NewItem()
	err := f.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return cdl.Err(fmt.Errorf("could not map entity to item because %s", err.Error()), cdl.LayerErrorInternal)
	}

	err = f.enc.Write(item)
	if err != nil {
		return cdl.Err(fmt.Errorf("could not write item to file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	return nil
}

func (f FileSystemDatasetWriter) Close() cdl.LayerError {
	err := f.enc.Close()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not close file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	if f.closeFullSync {
		err = os.Rename(f.tmpFullSyncPath, f.fullSyncFilePath)
		if err != nil {
			return cdl.Err(fmt.Errorf("could not rename file because %s", err.Error()), cdl.LayerErrorInternal)
		}
	}

	return nil
}

type FileInfo struct {
	Entry fs.DirEntry
	Path  string
}

func (f FileSystemDataset) Changes(since string, limit int, latestOnly bool) (cdl.EntityIterator, cdl.LayerError) {
	// get root folder
	if _, err := os.Stat(f.config.ReadPath); os.IsNotExist(err) {
		return nil, cdl.Err(fmt.Errorf("path %s does not exist", f.config.ReadPath), cdl.LayerErrorBadParameter)
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
			return nil, cdl.Err(fmt.Errorf("could not read directory %s", f.config.ReadPath), cdl.LayerErrorBadParameter)
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
			return nil, cdl.Err(fmt.Errorf("could not read directory %s", folder), cdl.LayerErrorBadParameter)
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
			return nil, cdl.Err(fmt.Errorf("could not match file pattern %s", f.config.ReadFilePattern), cdl.LayerErrorInternal)
		}

		if isMatch {
			if f.config.SupportSinceByFileTimestamp && since != "" {
				finfo, err := file.Entry.Info()
				if err != nil {
					return nil, cdl.Err(fmt.Errorf("could not get file info for %s", fileName), cdl.LayerErrorInternal)
				}
				fileModTime := finfo.ModTime().UnixMicro()
				sinceTimeAsInt, err := strconv.ParseInt(since, 10, 64)
				if err != nil {
					return nil, cdl.Err(fmt.Errorf("could not parse since time %s", since), cdl.LayerErrorInternal)
				}

				if sinceTimeAsInt < fileModTime {
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

	mapper := cdl.NewMapper(f.logger, nil, f.datasetDefinition.OutgoingMappingConfig)
	iterator := NewFileCollectionEntityIterator(f.datasetDefinition.SourceConfig, f.logger, dataFileInfos, mapper, "")
	return iterator, nil
}

func (f FileSystemDataset) Entities(from string, limit int) (cdl.EntityIterator, cdl.LayerError) {
	return nil, cdl.Err(fmt.Errorf("operation not supported"), cdl.LayerNotSupported)
}

func NewFileCollectionEntityIterator(sourceConfig map[string]any, logger cdl.Logger, files []FileInfo, mapper *cdl.Mapper, token string) *FileCollectionEntityIterator {
	return &FileCollectionEntityIterator{sourceConfig: sourceConfig, mapper: mapper, token: token, files: files, filesIndex: 0, logger: logger}
}

type FileCollectionEntityIterator struct {
	mapper            *cdl.Mapper
	token             string
	files             []FileInfo
	filesIndex        int
	currentItemReader encoder.ItemIterator
	sourceConfig      map[string]any
	logger            cdl.Logger
}

func (f *FileCollectionEntityIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (f *FileCollectionEntityIterator) Next() (*egdm.Entity, cdl.LayerError) {
	if f.currentItemReader == nil {
		if f.filesIndex < len(f.files) {
			// initialize the current file entity iterator
			fileInfo := f.files[f.filesIndex]
			file := filepath.Join(fileInfo.Path, fileInfo.Entry.Name())

			// get modified time for the file
			info, err := fileInfo.Entry.Info()
			f.token = strconv.FormatInt(info.ModTime().UnixMicro(), 10)

			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), cdl.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
		} else {
			return nil, nil
		}
	}

	// read the next entity from the current file
	item, err := f.currentItemReader.Read()
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not read item from file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	if item == nil {
		// close the current file and move to the next
		err := f.currentItemReader.Close()
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not close item reader for file because %s", err.Error()), cdl.LayerErrorInternal)
		}
		f.filesIndex++
		if f.filesIndex < len(f.files) {
			fileInfo := f.files[f.filesIndex]
			file := filepath.Join(fileInfo.Path, fileInfo.Entry.Name())

			info, err := fileInfo.Entry.Info()
			f.token = strconv.FormatInt(info.ModTime().UnixMicro(), 10)

			itemReader, err := f.NewItemReadCloser(file, f.sourceConfig)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not create item reader for file %s becuase %s", file, err.Error()), cdl.LayerErrorInternal)
			}

			f.currentItemReader = itemReader
			item, err = f.currentItemReader.Read()
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not read item from file because %s", err.Error()), cdl.LayerErrorInternal)
			}
		}
	}

	if item == nil {
		return nil, nil
	} else {
		entity := egdm.NewEntity()
		err := f.mapper.MapItemToEntity(item, entity)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not map item to entity because %s", err.Error()), cdl.LayerErrorInternal)
		}
		return entity, nil
	}
}

func (f *FileCollectionEntityIterator) NewItemReadCloser(filePath string, sourceConfig map[string]any) (encoder.ItemIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not open file %s", filePath), cdl.LayerErrorInternal)
	}

	// get encoder for the file
	itemReader, err := encoder.NewItemIterator(sourceConfig, f.logger, file)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not create encoder specified in dataset source testconfig"), cdl.LayerErrorBadParameter)
	}

	return itemReader, nil
}

func (f *FileCollectionEntityIterator) Token() (*egdm.Continuation, cdl.LayerError) {
	cont := egdm.NewContinuation()
	cont.Token = f.token
	return cont, nil
}

func (f *FileCollectionEntityIterator) Close() cdl.LayerError {
	err := f.currentItemReader.Close()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not close item reader because %s", err.Error()), cdl.LayerErrorInternal)
	}
	return nil
}
