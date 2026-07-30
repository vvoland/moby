package wire

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ToDynamic converts a contract message -- a pointer to one of the point's
// message structs -- into a protobuf message.
//
// The result is an ordinary [proto.Message], so it travels over gRPC through the
// stock protobuf codec: the traffic is standard gRPC with the standard
// application/grpc+proto content type, and an extension written in another
// language talks to it with ordinary stubs generated from the point's .proto. No
// custom codec is registered and no wire framing is invented here.
//
// Fields at their zero value are omitted, which is proto3's implicit presence:
// the same bytes a generated marshaller emits, and what makes adding a field
// backwards compatible.
func (c *Contract) ToDynamic(v any) (*dynamicpb.Message, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return nil, fmt.Errorf("wire: want a non-nil pointer to a message struct, got %T", v)
	}
	p, ok := c.plans[rv.Type().Elem()]
	if !ok {
		return nil, fmt.Errorf("wire: %s is not a message of point %q", rv.Type().Elem(), c.PointID)
	}
	msg := dynamicpb.NewMessage(p.desc)
	if err := setMessage(msg, p, rv.Elem()); err != nil {
		return nil, err
	}
	return msg, nil
}

// FromDynamic converts a protobuf message back into a contract message. v must
// be a non-nil pointer to one of the point's message structs.
//
// Unknown fields are ignored, so an extension built against a newer revision of
// the contract can send fields this daemon does not know about without breaking
// the call.
func (c *Contract) FromDynamic(msg proto.Message, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("wire: want a non-nil pointer to a message struct, got %T", v)
	}
	p, ok := c.plans[rv.Type().Elem()]
	if !ok {
		return fmt.Errorf("wire: %s is not a message of point %q", rv.Type().Elem(), c.PointID)
	}
	return getMessage(msg.ProtoReflect(), p, rv.Elem())
}

// NewRequest returns an empty protobuf message for a method's request, to decode
// an incoming call into, along with a freshly allocated contract value of the
// matching Go type to convert it to. It is how the generic server handler
// receives a call without knowing the concrete types at compile time.
func (c *Contract) NewRequest(method string) (*dynamicpb.Message, any, error) {
	mp, ok := c.byMethod[method]
	if !ok {
		return nil, nil, fmt.Errorf("wire: point %q has no method %q", c.PointID, method)
	}
	return dynamicpb.NewMessage(mp.reqDesc), reflect.New(mp.method.Request).Interface(), nil
}

// NewResponse returns an empty protobuf message for a method's response, to
// decode a reply into. For a method whose Go signature returns only error, this
// is the synthesized empty response message, which carries no fields.
func (c *Contract) NewResponse(method string) (*dynamicpb.Message, error) {
	mp, ok := c.byMethod[method]
	if !ok {
		return nil, fmt.Errorf("wire: point %q has no method %q", c.PointID, method)
	}
	return dynamicpb.NewMessage(mp.respDesc), nil
}

// Marshal encodes a contract message to protobuf wire bytes. It is the
// convenience form of [Contract.ToDynamic] followed by [proto.Marshal], used by
// tests that assert byte-level compatibility with generated code.
func (c *Contract) Marshal(v any) ([]byte, error) {
	msg, err := c.ToDynamic(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(msg)
}

// Unmarshal decodes protobuf wire bytes into a contract message.
func (c *Contract) Unmarshal(data []byte, v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("wire: want a non-nil pointer to a message struct, got %T", v)
	}
	p, ok := c.plans[rv.Type().Elem()]
	if !ok {
		return fmt.Errorf("wire: %s is not a message of point %q", rv.Type().Elem(), c.PointID)
	}
	msg := dynamicpb.NewMessage(p.desc)
	if err := proto.Unmarshal(data, msg); err != nil {
		return err
	}
	return getMessage(msg, p, rv.Elem())
}

// setMessage copies a Go struct value into a dynamic message.
func setMessage(dst *dynamicpb.Message, p *msgPlan, v reflect.Value) error {
	for _, f := range p.fields {
		fv := v.Field(f.index)
		switch f.kind {
		case scalarSingle:
			// proto3 implicit presence: a zero scalar is not on the wire.
			if fv.IsZero() {
				continue
			}
			pv, err := scalarToProto(f.desc, fv)
			if err != nil {
				return err
			}
			dst.Set(f.desc, pv)

		case scalarRepeated:
			if fv.Len() == 0 {
				continue
			}
			list := dst.Mutable(f.desc).List()
			for i := 0; i < fv.Len(); i++ {
				pv, err := scalarToProto(f.desc, fv.Index(i))
				if err != nil {
					return err
				}
				list.Append(pv)
			}

		case scalarMap:
			if fv.Len() == 0 {
				continue
			}
			m := dst.Mutable(f.desc).Map()
			valDesc := f.desc.MapValue()
			iter := fv.MapRange()
			for iter.Next() {
				pv, err := scalarToProto(valDesc, iter.Value())
				if err != nil {
					return err
				}
				m.Set(protoreflect.ValueOfString(iter.Key().String()).MapKey(), pv)
			}

		case messageSingle:
			if fv.IsNil() {
				continue
			}
			sub := dynamicpb.NewMessage(f.elem.desc)
			if err := setMessage(sub, f.elem, fv.Elem()); err != nil {
				return err
			}
			dst.Set(f.desc, protoreflect.ValueOfMessage(sub))

		case messageRepeated:
			if fv.Len() == 0 {
				continue
			}
			list := dst.Mutable(f.desc).List()
			for i := 0; i < fv.Len(); i++ {
				sub := dynamicpb.NewMessage(f.elem.desc)
				if err := setMessage(sub, f.elem, fv.Index(i)); err != nil {
					return err
				}
				list.Append(protoreflect.ValueOfMessage(sub))
			}
		}
	}
	return nil
}

// getMessage copies a dynamic message into a Go struct value.
func getMessage(src protoreflect.Message, p *msgPlan, v reflect.Value) error {
	for _, f := range p.fields {
		fv := v.Field(f.index)
		if !src.Has(f.desc) && f.kind != scalarSingle {
			continue
		}
		switch f.kind {
		case scalarSingle:
			// Absent scalars read back as the descriptor's zero value, which is
			// the Go zero value too, so there is nothing to do for them.
			if !src.Has(f.desc) {
				fv.Set(reflect.Zero(fv.Type()))
				continue
			}
			if err := scalarFromProto(f.desc, src.Get(f.desc), fv); err != nil {
				return err
			}

		case scalarRepeated:
			list := src.Get(f.desc).List()
			out := reflect.MakeSlice(fv.Type(), list.Len(), list.Len())
			for i := 0; i < list.Len(); i++ {
				if err := scalarFromProto(f.desc, list.Get(i), out.Index(i)); err != nil {
					return err
				}
			}
			fv.Set(out)

		case scalarMap:
			m := src.Get(f.desc).Map()
			out := reflect.MakeMapWithSize(fv.Type(), m.Len())
			valDesc := f.desc.MapValue()
			var err error
			m.Range(func(k protoreflect.MapKey, val protoreflect.Value) bool {
				ev := reflect.New(fv.Type().Elem()).Elem()
				if err = scalarFromProto(valDesc, val, ev); err != nil {
					return false
				}
				out.SetMapIndex(reflect.ValueOf(k.String()).Convert(fv.Type().Key()), ev)
				return true
			})
			if err != nil {
				return err
			}
			fv.Set(out)

		case messageSingle:
			sub := reflect.New(fv.Type().Elem())
			if err := getMessage(src.Get(f.desc).Message(), f.elem, sub.Elem()); err != nil {
				return err
			}
			fv.Set(sub)

		case messageRepeated:
			list := src.Get(f.desc).List()
			out := reflect.MakeSlice(fv.Type(), list.Len(), list.Len())
			for i := 0; i < list.Len(); i++ {
				if err := getMessage(list.Get(i).Message(), f.elem, out.Index(i)); err != nil {
					return err
				}
			}
			fv.Set(out)
		}
	}
	return nil
}

func scalarToProto(fd protoreflect.FieldDescriptor, v reflect.Value) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(v.String()), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes(v.Bytes()), nil
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(v.Bool()), nil
	case protoreflect.Int32Kind:
		return protoreflect.ValueOfInt32(int32(v.Int())), nil
	case protoreflect.Int64Kind:
		return protoreflect.ValueOfInt64(v.Int()), nil
	case protoreflect.Uint32Kind:
		return protoreflect.ValueOfUint32(uint32(v.Uint())), nil
	case protoreflect.Uint64Kind:
		return protoreflect.ValueOfUint64(v.Uint()), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(float32(v.Float())), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(v.Float()), nil
	}
	return protoreflect.Value{}, fmt.Errorf("wire: unsupported proto kind %s", fd.Kind())
}

func scalarFromProto(fd protoreflect.FieldDescriptor, pv protoreflect.Value, dst reflect.Value) error {
	switch fd.Kind() {
	case protoreflect.StringKind:
		dst.SetString(pv.String())
	case protoreflect.BytesKind:
		// Copy: the decoded buffer is owned by the dynamic message, and a
		// contract value outlives the call that produced it.
		b := pv.Bytes()
		out := make([]byte, len(b))
		copy(out, b)
		dst.SetBytes(out)
	case protoreflect.BoolKind:
		dst.SetBool(pv.Bool())
	case protoreflect.Int32Kind, protoreflect.Int64Kind:
		dst.SetInt(pv.Int())
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		dst.SetUint(pv.Uint())
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		dst.SetFloat(pv.Float())
	default:
		return fmt.Errorf("wire: unsupported proto kind %s", fd.Kind())
	}
	return nil
}
