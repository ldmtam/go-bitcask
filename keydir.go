package gobitcask

import (
	"bytes"
	"hash/crc32"
	"os"
	"path"
	"sync"
)

type Entry struct {
	FileID    string
	ValueSize int
	ValuePos  int
	Timestamp uint32
}

type KeyDir struct {
	kd map[string]*Entry
	mu sync.RWMutex
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		kd: make(map[string]*Entry),
	}
}

func (k *KeyDir) Set(key []byte, entry *Entry) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.kd[string(key)] = entry
}

func (k *KeyDir) Get(key []byte) (*Entry, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	entry, ok := k.kd[string(key)]
	return entry, ok
}

func (k *KeyDir) Delete(key []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()

	delete(k.kd, string(key))
}

func (k *KeyDir) GetKeys() [][]byte {
	k.mu.Lock()
	defer k.mu.Unlock()

	keys := make([][]byte, 0, len(k.kd))
	for key := range k.kd {
		keys = append(keys, []byte(key))
	}

	return keys
}

func (k *KeyDir) WarmUp(dirName string, filesName []string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, fileName := range filesName {
		filePath := path.Join(dirName, fileName)
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		buf := bytes.NewBuffer(data)
		offset := 0

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

			// encode data
			data, err := encodeRawData(key, val, uint32ToBytes(ts))
			if err != nil {
				return err
			}

			if checksum != crc32.ChecksumIEEE(data) {
				return ErrChecksumNotMatch
			}

			offset += checksumLen + tsLen + keySizeLen + valueSizeLen + int(keySize)

			k.kd[string(key)] = &Entry{
				FileID:    fileName,
				ValueSize: int(valueSize),
				ValuePos:  offset,
				Timestamp: ts,
			}

			offset += int(valueSize)
		}
	}

	return nil
}

func (k *KeyDir) Merge(k2 *KeyDir) {
	k.mu.Lock()
	defer k.mu.Unlock()

	for key, entry := range k2.kd {
		k.kd[string(key)] = entry
	}
}
