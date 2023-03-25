package rabbitmq

type Options struct {
	// 是否自动确认消息
	AutoAck bool

	// 消息队列名。如果为空rabbitmq服务器回创建一个唯一的队列名
	Queue string

	// 路由key
	Key string

	// 持久化队列。rabbitmq重启后消息不会丢失
	DurableQueue bool

	//  设置当autoAck为false时，消费失败将消息重入队列
	Requeue bool

	// 交换机配置
	Exchange ExchangeOptions
	// 是否需要申明交换机，true表示不需要
	WithoutExchange bool
}

type Option func(options *Options)

func newOptions(opts ...Option) Options {
	opt := Options{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

// Requeue 设置消费失败时将消息重入队列
func Requeue() Option {
	return func(o *Options) {
		o.Requeue = true
	}
}

// Queue 设置队列名
func Queue(name string) Option {
	return func(o *Options) {
		o.Queue = name
	}
}

// DurableQueue 设置为持久化队列
func DurableQueue() func(o *Options) {
	return func(o *Options) {
		o.DurableQueue = true
	}
}

// Key 设置路由key
func Key(key string) func(o *Options) {
	return func(o *Options) {
		o.Key = key
	}
}

// DisableAutoAck 禁止自动确认消息
func DisableAutoAck() Option {
	return func(o *Options) {
		o.AutoAck = false
	}
}

// Exchange 设置交换机配置
func Exchange(ex ExchangeOptions) Option {
	return func(o *Options) {
		o.Exchange = ex
	}
}

// WithoutExchange 不申明交换机
func WithoutExchange() Option {
	return func(o *Options) {
		o.WithoutExchange = true
	}
}
