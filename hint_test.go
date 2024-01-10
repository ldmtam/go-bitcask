package gobitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateNewHint(t *testing.T) {
	err := os.MkdirAll("test", 0775)
	assert.Nil(t, err)
	defer os.RemoveAll("test")

	h, err := NewHint("test", "1.hint")
	assert.Nil(t, err)
	assert.NotNil(t, h)
}

func TestSimpleWriteReadHint(t *testing.T) {
	err := os.MkdirAll("test", 0775)
	assert.Nil(t, err)
	defer os.RemoveAll("test")

	h, err := NewHint("test", "1.hint")
	assert.Nil(t, err)
	assert.NotNil(t, h)

	keyDir := NewKeyDir()
	key := []byte("key1")
	entry := &Entry{
		FileID:    "1.hint",
		ValueSize: 2,
		ValuePos:  3,
		Timestamp: uint32(1234),
	}

	keyDir.Set(key, entry)

	err = h.Write(keyDir)
	assert.Nil(t, err)

	err = h.Close()
	assert.Nil(t, err)

	h2, err := OpenHint("test", "1.hint")
	assert.Nil(t, err)
	assert.NotNil(t, h2)

	readKeyDir, err := h2.Read()
	assert.Nil(t, err)
	assert.NotNil(t, readKeyDir)

	fetchedEntry, exist := readKeyDir.Get(key)
	assert.True(t, exist)
	assert.EqualValues(t, entry, fetchedEntry)
}
