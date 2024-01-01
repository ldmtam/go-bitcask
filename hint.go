package gobitcask

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
)

type Hint struct {
	f        *os.File
	id       string
	readOnly bool
}

func NewHint(dir, id string) (*Hint, error) {
	filePath := path.Join(dir, id)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}

	return &Hint{
		f:  f,
		id: id,
	}, nil
}

func OpenHint(dir, id string) (*Hint, error) {
	filePath := path.Join(dir, id)
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	return &Hint{
		f:        f,
		id:       id,
		readOnly: true,
	}, nil
}

func (h *Hint) Write(keyDir *KeyDir) error {
	if h.readOnly {
		return errors.New("can't write to read-only hint")
	}

	for key, entry := range keyDir.kd {
		rawHint, err := encodeRawHint([]byte(key), entry)
		if err != nil {
			return err
		}

		_, err = h.f.Write(rawHint)
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *Hint) Read() (*KeyDir, error) {
	buf := bytes.NewBuffer(nil)

	_, err := io.Copy(buf, h.f)
	if err != nil {
		return nil, err
	}

	keyDir := NewKeyDir()

	for buf.Len() > 0 {
		// get timestamp
		ts := bytesToUint32(buf.Next(tsLen))

		// get key size
		keySize := bytesToUint32(buf.Next(keySizeLen))

		// get value size
		valueSize := bytesToUint32(buf.Next(valueSizeLen))

		// get value position
		valuePos := bytesToUint32(buf.Next(8))

		// get key
		key := buf.Next(int(keySize))

		entry := &Entry{
			FileID:    h.id,
			ValueSize: int(valueSize),
			ValuePos:  int(valuePos),
			Timestamp: ts,
		}

		keyDir.Set(key, entry)
	}

	return keyDir, nil
}

func (h *Hint) Close() error {
	err := h.f.Sync()
	if err != nil {
		return err
	}

	return h.f.Close()
}

func encodeRawHint(key []byte, entry *Entry) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	_, err := buf.Write(uint32ToBytes(entry.Timestamp))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(uint32ToBytes(uint32(len(key))))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(uint64ToBytes(uint64(entry.ValueSize)))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(uint64ToBytes(uint64(entry.ValuePos)))
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(key)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
