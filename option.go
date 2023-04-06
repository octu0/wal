package wal

const (
	defaultSyncMode        bool   = false
	defaultCloseCompaction bool   = true
	defaultWriteBufferSize int    = 32 * 1024
	defaultMaxSegmentSize  uint64 = 100 * 1024 * 1024
)

type OptionFunc func(*logOpt)

type logOpt struct {
	sync            bool
	closeCompaction bool
	writeBufferSize int
	maxSegmentSize  uint64
	dataloadFunc    DataLoadFunc
	compactFunc     func() // for testing
}

func WithSync(enable bool) OptionFunc {
	return func(opt *logOpt) {
		opt.sync = enable
	}
}

func WithCloseCompaction(enable bool) OptionFunc {
	return func(opt *logOpt) {
		opt.closeCompaction = enable
	}
}

func WithWriteBufferSize(size int) OptionFunc {
	return func(opt *logOpt) {
		opt.writeBufferSize = size
	}
}

func WithMaxSegmentSize(size uint64) OptionFunc {
	return func(opt *logOpt) {
		opt.maxSegmentSize = size
	}
}

func WithDataLoadFunc(fn DataLoadFunc) OptionFunc {
	return func(opt *logOpt) {
		opt.dataloadFunc = fn
	}
}

func newLogOpt(funcs ...OptionFunc) *logOpt {
	opt := &logOpt{
		sync:            defaultSyncMode,
		closeCompaction: defaultCloseCompaction,
		writeBufferSize: defaultWriteBufferSize,
		maxSegmentSize:  defaultMaxSegmentSize,
		dataloadFunc:    nil,
		compactFunc:     nil,
	}
	for _, fn := range funcs {
		fn(opt)
	}
	return opt
}
