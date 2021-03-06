package clientinterceptors

import (
	"context"

	"github.com/tal-tech/go-zero/core/retry"
	"google.golang.org/grpc"
)

// RetryInterceptor retry interceptor
func RetryInterceptor(enable bool) grpc.UnaryClientInterceptor {
	if !enable {
		return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return retry.Do(ctx, func(ctx context.Context, callOpts ...grpc.CallOption) error {
			return invoker(ctx, method, req, reply, cc, callOpts...)
		}, opts...)
	}
}
