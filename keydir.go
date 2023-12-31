package gobitcask

import (
	"bytes"
	"hash/crc32"
	"os"
	"path"
)

type Entry struct {
	FileID    int
	ValueSize int
	ValuePos  int
	Timestamp uint32
}

type KeyDir struct {
	m map[string]*Entry
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		m: make(map[string]*Entry),
	}
}

func (k *KeyDir) Set(key []byte, entry *Entry) {
	k.m[string(key)] = entry
}

func (k *KeyDir) Get(key []byte) (*Entry, bool) {
	entry, ok := k.m[string(key)]
	return entry, ok
}

func (k *KeyDir) Delete(key []byte) {
	delete(k.m, string(key))
}

func (k *KeyDir) GetKeys() [][]byte {
	keys := make([][]byte, 0, len(k.m))
	for key := range k.m {
		keys = append(keys, []byte(key))
	}

	return keys
}

func (k *KeyDir) WarmUp(dirName string, filesName []string) error {
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
			data, err := encodeRaw(key, val, uint32ToBytes(ts))
			if err != nil {
				return err
			}

			if checksum != crc32.ChecksumIEEE(data) {
				return ErrChecksumNotMatch
			}

			offset += checksumLen + tsLen + keySizeLen + valueSizeLen + int(keySize)

			k.m[string(key)] = &Entry{
				FileID:    extractSegmentID(fileName),
				ValueSize: int(valueSize),
				ValuePos:  offset,
				Timestamp: ts,
			}

			offset += int(valueSize)
		}
	}

	return nil
}
