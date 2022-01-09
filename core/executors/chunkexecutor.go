package executors

import "time"

const defaultChunkSize = 1024 * 1024 // 1M

type (
	// ChunkOption defines the method to customize a ChunkExecutor.
	ChunkOption func(options *chunkOptions)

	// A ChunkExecutor is an executor to execute tasks when either requirement meets:
	// 1. up to given chunk size
	// 2. flush interval elapsed
	ChunkExecutor struct {
		executor  *PeriodicalExecutor
		container *chunkContainer
	}
	// 可选配置
	chunkOptions struct {
		// 字节容量阈值
		chunkSize int
		// 周期性刷新时间,兜底策略
		flushInterval time.Duration
	}
)

// NewChunkExecutor returns a ChunkExecutor.
// 构造函数
// execute - 任务执行函数
// opts - 可选配置参数
func NewChunkExecutor(execute Execute, opts ...ChunkOption) *ChunkExecutor {
	options := newChunkOptions()
	for _, opt := range opts {
		opt(&options)
	}
	container := &chunkContainer{
		execute:      execute,
		maxChunkSize: options.chunkSize,
	}
	executor := &ChunkExecutor{
		executor:  NewPeriodicalExecutor(options.flushInterval, container),
		container: container,
	}

	return executor
}

// Add adds task with given chunk size into ce.
func (ce *ChunkExecutor) Add(task interface{}, size int) error {
	ce.executor.Add(chunk{
		val:  task,
		size: size,
	})
	return nil
}

// Flush forces ce to flush and execute tasks.
func (ce *ChunkExecutor) Flush() {
	ce.executor.Flush()
}

// Wait waits the execution to be done.
func (ce *ChunkExecutor) Wait() {
	ce.executor.Wait()
}

// WithChunkBytes customizes a ChunkExecutor with the given chunk size.
func WithChunkBytes(size int) ChunkOption {
	return func(options *chunkOptions) {
		options.chunkSize = size
	}
}

// WithFlushInterval customizes a ChunkExecutor with the given flush interval.
func WithFlushInterval(duration time.Duration) ChunkOption {
	return func(options *chunkOptions) {
		options.flushInterval = duration
	}
}

func newChunkOptions() chunkOptions {
	return chunkOptions{
		chunkSize:     defaultChunkSize,
		flushInterval: defaultFlushInterval,
	}
}

type chunkContainer struct {
	tasks        []interface{}
	execute      Execute
	size         int
	maxChunkSize int
}

func (bc *chunkContainer) AddTask(task interface{}) bool {
	ck := task.(chunk)
	bc.tasks = append(bc.tasks, ck.val)
	bc.size += ck.size
	return bc.size >= bc.maxChunkSize
}

func (bc *chunkContainer) Execute(tasks interface{}) {
	vals := tasks.([]interface{})
	bc.execute(vals)
}

func (bc *chunkContainer) RemoveAll() interface{} {
	tasks := bc.tasks
	bc.tasks = nil
	bc.size = 0
	return tasks
}

type chunk struct {
	val  interface{}
	size int
}
