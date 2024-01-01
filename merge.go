package gobitcask

import (
	"bytes"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

type DiskEntry struct {
	Checksum uint32
	Ts       uint32
	Key      []byte
	Value    []byte
}

type Merger struct {
	dir      string
	keyDir   *KeyDir
	mergeOpt *MergeOption
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewMerger(dir string, keyDir *KeyDir, mergeOpt *MergeOption) *Merger {
	return &Merger{
		dir:      dir,
		keyDir:   keyDir,
		mergeOpt: mergeOpt,
		stopCh:   make(chan struct{}),
	}
}

func (m *Merger) Start() {
	m.wg.Add(1)
	defer m.wg.Done()

	ch := time.Tick(m.mergeOpt.Interval)

	for {
		select {
		case <-ch:
			mergedFiles, lastSegmentName, err := m.getMergeFilesName()
			if err == ErrNotEnoughDataFiles {
				continue
			} else if err != nil {
				panic(err) // TODO: should handle this error properly
			}

			mergedKeyDir, err := m.mergeData(mergedFiles, lastSegmentName)
			if err != nil {
				panic(err) // TODO: should handle this error properly
			}

			m.keyDir.Merge(mergedKeyDir)

			err = m.createHintFile(lastSegmentName, mergedKeyDir)
			if err != nil {
				panic(err) // TODO: should handle this error properly
			}

			for _, mergeFile := range mergedFiles {
				mergeFilePath := path.Join(m.dir, mergeFile)
				err = os.RemoveAll(mergeFilePath)
				if err != nil {
					panic(err) // TODO: should handle this error properly
				}
			}

		case <-m.stopCh:
			return
		}
	}
}

func (m *Merger) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *Merger) getMergeFilesName() ([]string, string, error) {
	dirEntries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, "", err
	}
	dirEntries = dirEntries[:len(dirEntries)-1] // don't merge active segment

	fileNameMap := make(map[string]bool)
	for _, dirEntry := range dirEntries {
		fileNameMap[dirEntry.Name()] = true
	}

	var lastSegmentName string
	filesName := make([]string, 0)

	for idx, dirEntry := range dirEntries {
		fileName := dirEntry.Name()
		if idx == len(dirEntries)-1 {
			lastSegmentName = fileName
		}

		// skip hint files
		if path.Ext(fileName) == ".hint" {
			continue
		}

		// skip data files which is already hinted
		id := extractID(fileName)
		if fileNameMap[getHintFilename(id)] {
			continue
		}

		filesName = append(filesName, fileName)
	}

	if m.mergeOpt.Min != 0 && len(filesName) < m.mergeOpt.Min {
		return nil, "", ErrNotEnoughDataFiles
	}

	return filesName, lastSegmentName, nil
}

func (m *Merger) mergeData(filesName []string, lastSegmentName string) (*KeyDir, error) {
	keyDir := NewKeyDir()

	diskEntryMap := make(map[string]*DiskEntry)
	for _, fileName := range filesName {
		filePath := path.Join(m.dir, fileName)
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, err
		}

		buf := bytes.NewBuffer(data)
		for buf.Len() > 0 {
			// get checksum
			checksum := bytesToUint32(buf.Next(checksumLen))

			// get ts
			ts := bytesToUint32(buf.Next(tsLen))

			// get key size
			keySize := bytesToUint32(buf.Next(keySizeLen))

			// get value size
			valueSize := bytesToUint64(buf.Next(valueSizeLen))

			// get key
			key := buf.Next(int(keySize))

			// get value
			val := buf.Next(int(valueSize))

			// key/value pair is deleted
			if bytes.Compare(val, tombstoneValue) == 0 {
				continue
			}

			diskEntryMap[string(key)] = &DiskEntry{
				Checksum: checksum,
				Ts:       ts,
				Key:      key,
				Value:    val,
			}
		}
	}

	diskEntries := make([]*DiskEntry, 0, len(diskEntryMap))
	for _, diskEntry := range diskEntryMap {
		diskEntries = append(diskEntries, diskEntry)
	}

	sort.Slice(diskEntries, func(i, j int) bool {
		return diskEntries[i].Ts < diskEntries[j].Ts
	})

	mergeFilename := getMergeFilename(extractID(lastSegmentName))
	mergeSegment, err := NewSegment(m.dir, mergeFilename)
	if err != nil {
		return nil, err
	}

	for _, diskEntry := range diskEntries {
		data, err := encode(diskEntry.Key, diskEntry.Value, uint32ToBytes(diskEntry.Ts))
		if err != nil {
			return nil, err
		}

		offset, err := mergeSegment.GetOffset()
		if err != nil {
			return nil, err
		}

		err = mergeSegment.Write(offset, data)
		if err != nil {
			return nil, err
		}
	}

	err = keyDir.WarmUp(m.dir, []string{mergeFilename})
	if err != nil {
		return nil, err
	}

	return keyDir, nil
}

func (m *Merger) createHintFile(id string, keyDir *KeyDir) error {
	hint, err := NewHint(m.dir, getHintFilename(extractID(id)))
	if err != nil {
		return err
	}

	return hint.Write(keyDir)
}
