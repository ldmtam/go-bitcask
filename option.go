package gobitcask

type OptFn func(*Option)

type Option struct {
	DirName     string
	SegmentSize int
}

func WithDirName(dirName string) OptFn {
	return func(o *Option) {
		o.DirName = dirName
	}
}

func WithSegmentSize(segmentSize int) OptFn {
	return func(o *Option) {
		o.SegmentSize = segmentSize
	}
}
