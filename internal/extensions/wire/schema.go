package wire

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Proto renders the point's contract as a .proto file.
//
// Nothing in the daemon reads this: the wire form is derived from the Go types,
// so the schema is documentation, not a build input. It exists because a point's
// contract has to be publishable -- an extension author working in another
// language needs a schema to generate stubs from, and "read the Go source" is
// not an answer for them. Points check the rendered output against a file in
// their package, so the published schema cannot drift from the contract without
// failing a test.
//
// There is deliberately no go_package option: no Go is generated from this file.
func (c *Contract) Proto() string {
	var b strings.Builder
	fmt.Fprintf(&b, "// Generated from the Go contract of %s. DO NOT EDIT.\n\n", c.PointID)
	fmt.Fprintf(&b, "syntax = \"proto3\";\n\npackage %s;\n\n", c.PointID)

	fmt.Fprintf(&b, "service %s {\n", c.Service)
	svc := c.file.Services().Get(0)
	for i := 0; i < svc.Methods().Len(); i++ {
		m := svc.Methods().Get(i)
		fmt.Fprintf(&b, "  rpc %s(%s) returns (%s);\n", m.Name(), m.Input().Name(), m.Output().Name())
	}
	b.WriteString("}\n")

	msgs := c.file.Messages()
	for i := 0; i < msgs.Len(); i++ {
		writeMessage(&b, msgs.Get(i))
	}
	return b.String()
}

func writeMessage(b *strings.Builder, md protoreflect.MessageDescriptor) {
	fmt.Fprintf(b, "\nmessage %s {\n", md.Name())
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		switch {
		case f.IsMap():
			fmt.Fprintf(b, "  map<%s, %s> %s = %d;\n",
				scalarName(f.MapKey()), scalarName(f.MapValue()), f.Name(), f.Number())
		case f.IsList():
			fmt.Fprintf(b, "  repeated %s %s = %d;\n", typeName(f), f.Name(), f.Number())
		default:
			fmt.Fprintf(b, "  %s %s = %d;\n", typeName(f), f.Name(), f.Number())
		}
	}
	b.WriteString("}\n")
}

// typeName is the proto type token for a field: the message name for a message
// field, the scalar keyword otherwise.
func typeName(f protoreflect.FieldDescriptor) string {
	if f.Kind() == protoreflect.MessageKind {
		return string(f.Message().Name())
	}
	return scalarName(f)
}

func scalarName(f protoreflect.FieldDescriptor) string {
	switch f.Kind() {
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.Int32Kind:
		return "int32"
	case protoreflect.Int64Kind:
		return "int64"
	case protoreflect.Uint32Kind:
		return "uint32"
	case protoreflect.Uint64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	}
	return f.Kind().String()
}
