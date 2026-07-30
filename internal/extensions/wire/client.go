package wire

import (
	"context"
	"fmt"
	"reflect"

	"google.golang.org/grpc"
)

// Invoke calls one of the point's methods on an out-of-process provider over
// conn.
//
// req is a pointer to the method's request struct. resp is a pointer to its
// response struct, or nil for a method whose Go signature returns only error.
// The call is an ordinary unary gRPC request carrying an ordinary protobuf
// message, so nothing about the traffic reveals that the contract was derived
// rather than generated.
func Invoke(ctx context.Context, conn grpc.ClientConnInterface, c *Contract, method string, req, resp any) error {
	reqMsg, err := c.ToDynamic(req)
	if err != nil {
		return err
	}
	respMsg, err := c.NewResponse(method)
	if err != nil {
		return err
	}
	if err := conn.Invoke(ctx, c.FullMethod(method), reqMsg, respMsg); err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	return c.FromDynamic(respMsg, resp)
}

// Client pairs a point's contract with a connection to a provider of it.
//
// A point's client adapter embeds one so that each of its methods is a single
// forwarding call: the contract and the connection are already bound, leaving
// only the method name and the request.
type Client struct {
	Contract *Contract
	Conn     grpc.ClientConnInterface
}

// Call invokes a method that answers with a response.
//
//	func (c client) Mount(ctx context.Context, req *MountRequest) (*PathResponse, error) {
//		return wire.Call[PathResponse](ctx, c.Client, "Mount", req)
//	}
func Call[R any](ctx context.Context, c Client, method string, req any) (*R, error) {
	var resp R
	if err := Invoke(ctx, c.Conn, c.Contract, method, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Do invokes a method that answers with only an error.
func Do(ctx context.Context, c Client, method string, req any) error {
	return Invoke(ctx, c.Conn, c.Contract, method, req, nil)
}

// BindFuncs fills a struct of function fields -- one per point method, named
// after it -- with clients that call the corresponding method over conn.
//
// This is how a point whose contract is a struct of funcs is reached across the
// process boundary with no generated code at all: Go cannot synthesize a value
// implementing an interface at runtime, but it can build a function, so a
// contract expressed as functions needs no per-point adapter.
//
// dst must be a non-nil pointer to a struct whose fields match the point's
// methods in name and signature.
func (c *Contract) BindFuncs(conn grpc.ClientConnInterface, dst any) error {
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("wire: BindFuncs wants a non-nil pointer to a struct, got %T", dst)
	}
	sv := rv.Elem()
	st := sv.Type()
	for _, m := range c.methods {
		field, ok := st.FieldByName(m.Name)
		if !ok {
			return fmt.Errorf("wire: point %q: %s has no field %s", c.PointID, st, m.Name)
		}
		if field.Type.Kind() != reflect.Func {
			return fmt.Errorf("wire: point %q: %s.%s is not a func", c.PointID, st, m.Name)
		}
		if err := checkFuncField(c.PointID, st.Name(), m, field.Type); err != nil {
			return err
		}
		fv := sv.FieldByIndex(field.Index)
		method := m
		call := func(ctx context.Context, req, resp any) error {
			return Invoke(ctx, conn, c, method.Name, req, resp)
		}
		fv.Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) []reflect.Value {
			ctx := args[0].Interface().(context.Context)
			req := args[1].Interface()
			if method.Response == nil {
				err := call(ctx, req, nil)
				return []reflect.Value{errValue(err)}
			}
			resp := reflect.New(method.Response)
			err := call(ctx, req, resp.Interface())
			if err != nil {
				return []reflect.Value{reflect.Zero(reflect.PointerTo(method.Response)), errValue(err)}
			}
			return []reflect.Value{resp, errValue(nil)}
		}))
	}
	return nil
}

// checkFuncField verifies that a contract struct's function field has the
// signature the point declares, so a mismatch is a startup error rather than a
// panic inside the reflect-built function on the first call.
func checkFuncField(pointID, structName string, m Method, ft reflect.Type) error {
	wantIn := []reflect.Type{ctxType, reflect.PointerTo(m.Request)}
	if ft.NumIn() != len(wantIn) {
		return fmt.Errorf("wire: point %q: %s.%s takes %d arguments, want %d", pointID, structName, m.Name, ft.NumIn(), len(wantIn))
	}
	for i, want := range wantIn {
		if ft.In(i) != want {
			return fmt.Errorf("wire: point %q: %s.%s argument %d is %s, want %s", pointID, structName, m.Name, i, ft.In(i), want)
		}
	}
	if m.Response == nil {
		if ft.NumOut() != 1 || ft.Out(0) != errType {
			return fmt.Errorf("wire: point %q: %s.%s must return error", pointID, structName, m.Name)
		}
		return nil
	}
	if ft.NumOut() != 2 || ft.Out(0) != reflect.PointerTo(m.Response) || ft.Out(1) != errType {
		return fmt.Errorf("wire: point %q: %s.%s must return (*%s, error)", pointID, structName, m.Name, m.Response.Name())
	}
	return nil
}

// errValue boxes err as a reflect.Value of static type error, which is what a
// reflect-built function must return even when the error is nil.
func errValue(err error) reflect.Value {
	if err == nil {
		return reflect.Zero(errType)
	}
	return reflect.ValueOf(err).Convert(errType)
}
