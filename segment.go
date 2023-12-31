package gobitcask

import (
	"errors"
	"os"
	"path"
)

type Segment struct {
	f        *os.File
	id       string
	readOnly bool
}

func OpenSegment(dir, id string) (*Segment, error) {
	filePath := path.Join(dir, id)
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}

	return &Segment{
		f:        f,
		id:       id,
		readOnly: true,
	}, nil
}

func NewSegment(dir, id string) (*Segment, error) {
	filePath := path.Join(dir, id)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return nil, err
	}

	return &Segment{
		f:  f,
		id: id,
	}, nil
}

func (s *Segment) Read(offset, n int) ([]byte, error) {
	b := make([]byte, n)

	_, err := s.f.ReadAt(b, int64(offset))
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *Segment) Write(offset int, b []byte) error {
	if s.readOnly {
		return errors.New("can't write to read-only segment")
	}

	_, err := s.f.WriteAt(b, int64(offset))
	if err != nil {
		return err
	}

	return nil
}

func (s *Segment) GetOffset() (int, error) {
	info, err := s.f.Stat()
	if err != nil {
		return 0, err
	}

	return int(info.Size()), nil
}

func (s *Segment) GetID() string {
	return s.id
}

func (s *Segment) Close() error {
	err := s.f.Sync()
	if err != nil {
		return err
	}

	return s.f.Close()
}
