package gobitcask

import "errors"

const (
	checksumLen  = 4
	tsLen        = 4
	keySizeLen   = 4
	valueSizeLen = 8
)

var (
	tombstoneValue = []byte("bItcA5k_49c266f9-1d18-41da-ab36-092da88e982a")
)

var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrOpenSegmentFailed = errors.New("open segment failed")
	ErrChecksumNotMatch  = errors.New("checksum not match")
)
