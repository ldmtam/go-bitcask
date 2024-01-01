package gobitcask

import "time"

type OptFn func(*Option)

type Option struct {
	DirName     string
	SegmentSize int
	MergeOpt    *MergeOption
}

type MergeOption struct {
	Interval time.Duration
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

func WithMergeOpt(mergeOpt *MergeOption) OptFn {
	return func(o *Option) {
		o.MergeOpt = mergeOpt
	}
}
