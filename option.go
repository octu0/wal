package wal

const (
	defaultSyncMode        bool = false
	defaultWriteBufferSize int  = 32 * 1024
)

type OptionFunc func(*logOpt)

type logOpt struct {
	sync            bool
	writeBufferSize int
	compactFunc     func()
}

func WithSync(enable bool) OptionFunc {
	return func(opt *logOpt) {
		opt.sync = enable
	}
}

func WithWriteBufferSize(size int) OptionFunc {
	return func(opt *logOpt) {
		opt.writeBufferSize = size
	}
}

func newLogOpt(funcs ...OptionFunc) *logOpt {
	opt := &logOpt{
		sync:            defaultSyncMode,
		writeBufferSize: defaultWriteBufferSize,
		compactFunc:     nil,
	}
	for _, fn := range funcs {
		fn(opt)
	}
	return opt
}
