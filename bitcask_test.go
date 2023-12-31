package gobitcask

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateNewBitcask(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(WithDirName(dirName))
	assert.Nil(t, err)
	assert.NotNil(t, bc)
}

func TestEncode(t *testing.T) {
	key := []byte("key1")
	val := []byte("val1")
	ts := uint32(time.Now().UnixNano())

	encodedData, err := encode(key, val, uint32ToBytes(ts))
	assert.Nil(t, err)
	assert.NotZero(t, len(encodedData))

	checksum, decodedTs, decodedKey, decodedVal := decode(encodedData)
	assert.EqualValues(t, key, decodedKey)
	assert.EqualValues(t, val, decodedVal)
	assert.EqualValues(t, ts, decodedTs)
	assert.NotZero(t, checksum)
}

func TestSimplePutGet(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key := []byte("key1")
	val := []byte("val1")

	err = bc.Put(key, val)
	assert.Nil(t, err)

	fetchedVal, err := bc.Get(key)
	assert.Nil(t, err)
	assert.EqualValues(t, val, fetchedVal)
}

func TestSimplePutDelete(t *testing.T) {
	dirName := "./test"
	defer os.RemoveAll(dirName)

	bc, err := New(
		WithDirName(dirName),
		WithSegmentSize(128), // bytes
	)
	assert.Nil(t, err)
	assert.NotNil(t, bc)

	key := []byte("key1")
	val := []byte("val1")

	err = bc.Put(key, val)
	assert.Nil(t, err)

	err = bc.Delete(key)
	assert.Nil(t, err)

	fetchedVal, err := bc.Get(key)
	assert.Nil(t, fetchedVal)
	assert.Equal(t, ErrKeyNotFound, err)
}
