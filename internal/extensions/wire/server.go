package wire

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
)

// Serve registers impl as the gRPC service for the point on r.
//
// The service descriptor is built from the contract at registration time, with
// one handler per method, so serving a point out of process needs no generated
// server code: the same [Contract] that describes the messages also dispatches
// the calls. impl is the point's Go interface value -- the very same value an
// in-process host would hold -- which is what keeps an extension's code
// independent of where it runs.
//
// It returns an error if impl does not implement the point's methods, so a
// mismatch is caught when the extension starts rather than when the daemon first
// calls it.
func Serve(r grpc.ServiceRegistrar, c *Contract, impl any) error {
	desc, err := ServiceDesc(c, impl)
	if err != nil {
		return err
	}
	r.RegisterService(desc, impl)
	return nil
}

// ServiceDesc builds the gRPC service descriptor for the point, bound to impl's
// method set. It is separated from [Serve] so a caller that needs the descriptor
// itself -- to report the service name it will serve, say -- can build it
// without registering.
func ServiceDesc(c *Contract, impl any) (*grpc.ServiceDesc, error) {
	implType := reflect.TypeOf(impl)
	if implType == nil {
		return nil, fmt.Errorf("wire: point %q: nil provider", c.PointID)
	}
	desc := &grpc.ServiceDesc{
		ServiceName: c.FullService(),
		// The handlers close over everything they need and read the provider from
		// the srv argument, so gRPC's own type check has nothing to enforce here.
		HandlerType: (*any)(nil),
		Metadata:    c.file.Path(),
	}
	for _, m := range c.methods {
		// Resolve the provider's method once, at registration, so the call path
		// is an already-bound reflect.Value rather than a name lookup.
		fn, err := providerMethod(implType, m)
		if err != nil {
			return nil, fmt.Errorf("wire: point %q: %w", c.PointID, err)
		}
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: m.Name,
			Handler:    c.handler(m, fn),
		})
	}
	return desc, nil
}

// providerMethod finds and type-checks the provider method backing one contract
// method.
func providerMethod(implType reflect.Type, m method) (reflect.Method, error) {
	fn, ok := implType.MethodByName(m.Name)
	if !ok {
		return reflect.Method{}, fmt.Errorf("provider %s has no method %s", implType, m.Name)
	}
	ft := fn.Type
	// A method obtained from a type (rather than a value) carries the receiver as
	// its first parameter, so the contract's parameters start at index 1.
	if ft.NumIn() != 3 || ft.In(1) != ctxType || ft.In(2) != reflect.PointerTo(m.Request) {
		return reflect.Method{}, fmt.Errorf("method %s: want (context.Context, *%s)", m.Name, m.Request.Name())
	}
	return fn, nil
}

// handler builds the gRPC handler for one method. It decodes the request into a
// dynamic message, converts it to the contract's Go type, calls the provider,
// and converts the result back.
func (c *Contract) handler(m method, fn reflect.Method) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		reqMsg, reqVal, err := c.NewRequest(m.Name)
		if err != nil {
			return nil, err
		}
		if err := dec(reqMsg); err != nil {
			return nil, err
		}
		if err := c.FromDynamic(reqMsg, reqVal); err != nil {
			return nil, err
		}

		call := func(ctx context.Context, req any) (any, error) {
			out := fn.Func.Call([]reflect.Value{
				reflect.ValueOf(srv),
				reflect.ValueOf(ctx),
				reflect.ValueOf(req),
			})
			// The signature was checked when the descriptor was built, so the
			// error is the last result either way, and a typed response is the
			// first of two.
			if err, _ := out[len(out)-1].Interface().(error); err != nil {
				return nil, err
			}
			if m.Response == nil {
				// A bare-error method: reply with the synthesized empty message,
				// which carries no fields and so encodes to no bytes.
				return c.NewResponse(m.Name)
			}
			if out[0].IsNil() {
				// A provider may return a nil response to mean "no change"; send
				// the empty message rather than a null pointer.
				return c.NewResponse(m.Name)
			}
			return c.ToDynamic(out[0].Interface())
		}

		if interceptor == nil {
			return call(ctx, reqVal)
		}
		return interceptor(ctx, reqVal, &grpc.UnaryServerInfo{
			Server:     srv,
			FullMethod: c.FullMethod(m.Name),
		}, call)
	}
}
