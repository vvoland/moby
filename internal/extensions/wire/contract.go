// Package wire derives an extension point's protobuf contract at runtime from
// the point's Go types.
//
// A point is a Go interface plus message structs whose fields carry pb:"N" tags
// giving their proto field numbers. Those types are the source of truth. Rather
// than generating a parallel set of protobuf structs and the conversions between
// them, this package builds the protobuf descriptors directly from the Go types
// and marshals the Go values against those descriptors. The bytes on the wire
// are identical to what protoc-gen-go would produce for the same contract, which
// [TestWireCompatibility] locks down against real generated types.
//
// The supported field shapes are deliberately narrow -- scalars, bytes, repeated
// scalars, string-keyed scalar maps, single messages, and repeated messages --
// and anything else is rejected when the contract is built, at daemon start,
// rather than producing something subtly wrong at call time.
package wire

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/moby/moby/v2/internal/extensions"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Method is one RPC on a point: the Go method name and the Go types of its
// request and response. Response is nil for a method whose Go signature returns
// only error, for which an empty response message is synthesized -- the same
// shape the point's Go interface already has.
type Method struct {
	Name     string
	Request  reflect.Type
	Response reflect.Type
}

// Contract is a point's wire contract: the protobuf descriptors derived from its
// Go types, plus the marshalling plans built from them. It is constructed once,
// when the point is registered, so the per-call path is a flat walk over
// precomputed field plans rather than struct-tag parsing.
type Contract struct {
	// PointID is the point id, which is also the proto package.
	PointID string
	// Service is the gRPC service name within that package. The full service
	// name on the wire is PointID + "." + Service.
	Service string

	methods []Method
	file    protoreflect.FileDescriptor
	plans   map[reflect.Type]*msgPlan
	// byMethod indexes the per-method descriptors used on the call path, so
	// dispatching a call is a map lookup rather than a scan.
	byMethod map[string]*methodPlan
}

// methodPlan holds the descriptors a single RPC needs: the request and response
// message descriptors the dynamic messages are built from.
type methodPlan struct {
	method   Method
	reqDesc  protoreflect.MessageDescriptor
	respDesc protoreflect.MessageDescriptor
}

// fieldKind is the shape of one contract field, resolved once at build time so
// the codec switches on an int rather than re-deriving it from reflect types.
type fieldKind int

const (
	scalarSingle fieldKind = iota
	scalarRepeated
	scalarMap
	messageSingle
	messageRepeated
)

// msgPlan is the marshalling plan for one message struct: its descriptor and one
// entry per pb-tagged field, in Go struct field order.
type msgPlan struct {
	goType reflect.Type
	desc   protoreflect.MessageDescriptor
	fields []fieldPlan
}

// fieldPlan binds a Go struct field to its proto field descriptor.
type fieldPlan struct {
	index int // index into the Go struct's fields
	desc  protoreflect.FieldDescriptor
	kind  fieldKind
	// elem is the plan for the message type of a messageSingle or
	// messageRepeated field.
	elem *msgPlan
}

// ctxType and errType are the fixed positions in a point method's signature.
var (
	ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errType = reflect.TypeOf((*error)(nil)).Elem()
)

// NewContractFor derives the wire contract for a point from its provider
// interface: the methods are read off the interface itself, so the Go interface
// really is the only place the contract is written down. There is no method list
// to keep in step with it and no way for the two to drift.
//
// Every method must be shaped
//
//	Name(context.Context, *Request) (*Response, error)
//	Name(context.Context, *Request) error
//
// and anything else is rejected here -- when the point is registered at daemon
// start -- rather than failing at call time.
func NewContractFor(pointID, service string, iface reflect.Type) (*Contract, error) {
	if iface.Kind() != reflect.Interface {
		return nil, fmt.Errorf("point %q: %s is not an interface", pointID, iface)
	}
	methods := make([]Method, 0, iface.NumMethod())
	for i := 0; i < iface.NumMethod(); i++ {
		m := iface.Method(i)
		ft := m.Type
		if ft.NumIn() != 2 || ft.In(0) != ctxType || ft.In(1).Kind() != reflect.Ptr {
			return nil, fmt.Errorf("point %q method %s: want (context.Context, *Request)", pointID, m.Name)
		}
		method := Method{Name: m.Name, Request: ft.In(1).Elem()}
		switch {
		case ft.NumOut() == 1 && ft.Out(0) == errType:
			// A bare-error method; an empty response message is synthesized.
		case ft.NumOut() == 2 && ft.Out(1) == errType && ft.Out(0).Kind() == reflect.Ptr:
			method.Response = ft.Out(0).Elem()
		default:
			return nil, fmt.Errorf("point %q method %s: result must be error or (*Response, error)", pointID, m.Name)
		}
		methods = append(methods, method)
	}
	return NewContract(pointID, service, methods)
}

// NewContract derives the wire contract for a point from its methods. pointID is
// the point id (used as the proto package) and service is the gRPC service name
// within it. It returns an error if any reachable message type uses a field
// shape the wire format does not support.
//
// Most callers want [NewContractFor], which reads the methods off the point's
// provider interface instead of taking them as data.
func NewContract(pointID, service string, methods []Method) (*Contract, error) {
	c := &Contract{PointID: pointID, Service: service, methods: methods}

	// Collect every message type reachable from the methods, in a stable order,
	// so the emitted descriptor (and the .proto artifact derived from it) does
	// not depend on map iteration order.
	var order []reflect.Type
	seen := map[reflect.Type]bool{}
	var walk func(reflect.Type) error
	walk = func(t reflect.Type) error {
		if t == nil || seen[t] {
			return nil
		}
		if t.Kind() != reflect.Struct {
			return fmt.Errorf("message type %s is not a struct", t)
		}
		seen[t] = true
		order = append(order, t)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if _, ok := f.Tag.Lookup("pb"); !ok {
				continue
			}
			if nested, ok := messageElem(f.Type); ok {
				if err := walk(nested); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for _, m := range methods {
		if err := walk(m.Request); err != nil {
			return nil, err
		}
		if m.Response != nil {
			if err := walk(m.Response); err != nil {
				return nil, err
			}
		}
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    strPtr(strings.ReplaceAll(pointID, ".", "/") + "/" + snake(service) + ".proto"),
		Package: strPtr(pointID),
		Syntax:  strPtr("proto3"),
	}
	for _, t := range order {
		dp, err := describeMessage(pointID, t)
		if err != nil {
			return nil, err
		}
		fdp.MessageType = append(fdp.MessageType, dp)
	}
	// Synthesize an empty response message for each bare-error method, matching
	// the shape the Go interface already implies.
	for _, m := range c.methods {
		if m.Response == nil {
			fdp.MessageType = append(fdp.MessageType, &descriptorpb.DescriptorProto{
				Name: strPtr(m.Name + "Response"),
			})
		}
	}
	svc := &descriptorpb.ServiceDescriptorProto{Name: strPtr(service)}
	for _, m := range c.methods {
		out := m.Name + "Response"
		if m.Response != nil {
			out = m.Response.Name()
		}
		svc.Method = append(svc.Method, &descriptorpb.MethodDescriptorProto{
			Name:       strPtr(m.Name),
			InputType:  strPtr("." + pointID + "." + m.Request.Name()),
			OutputType: strPtr("." + pointID + "." + out),
		})
	}
	fdp.Service = []*descriptorpb.ServiceDescriptorProto{svc}

	file, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, fmt.Errorf("build descriptor for point %q: %w", pointID, err)
	}
	c.file = file

	// Bind each Go type to its descriptor and precompute its field plan. This is
	// done after the file is built so nested message fields can point at the
	// finished descriptors.
	c.plans = make(map[reflect.Type]*msgPlan, len(order))
	for _, t := range order {
		md := file.Messages().ByName(protoreflect.Name(t.Name()))
		if md == nil {
			return nil, fmt.Errorf("message %s missing from built descriptor", t.Name())
		}
		c.plans[t] = &msgPlan{goType: t, desc: md}
	}
	for _, t := range order {
		if err := c.buildPlan(c.plans[t]); err != nil {
			return nil, err
		}
	}

	c.byMethod = make(map[string]*methodPlan, len(c.methods))
	for _, m := range c.methods {
		respName := m.Name + "Response"
		if m.Response != nil {
			respName = m.Response.Name()
		}
		reqDesc := file.Messages().ByName(protoreflect.Name(m.Request.Name()))
		respDesc := file.Messages().ByName(protoreflect.Name(respName))
		if reqDesc == nil || respDesc == nil {
			return nil, fmt.Errorf("method %q: request or response missing from built descriptor", m.Name)
		}
		c.byMethod[m.Name] = &methodPlan{method: m, reqDesc: reqDesc, respDesc: respDesc}
	}
	return c, nil
}

// File returns the derived file descriptor.
func (c *Contract) File() protoreflect.FileDescriptor { return c.file }

// Methods returns the contract's methods.
func (c *Contract) Methods() []Method { return c.methods }

// FullMethod returns the gRPC path for a method, as it appears on the wire:
// /<point-id>.<Service>/<Method>.
func (c *Contract) FullMethod(name string) string {
	return "/" + c.PointID + "." + c.Service + "/" + name
}

// FullService returns the fully-qualified gRPC service name.
func (c *Contract) FullService() string { return c.PointID + "." + c.Service }

func (c *Contract) buildPlan(p *msgPlan) error {
	t := p.goType
	byNumber := map[int]string{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup("pb")
		if !ok {
			continue
		}
		num, err := strconv.Atoi(tag)
		if err != nil {
			return fmt.Errorf("%s.%s: pb tag %q is not a number", t.Name(), f.Name, tag)
		}
		// Proto field numbers start at 1 and must be unique within the message; a
		// zero or a duplicate would produce a broken wire contract.
		if num < 1 {
			return fmt.Errorf("%s.%s: pb field number must be >= 1, got %d", t.Name(), f.Name, num)
		}
		if prev, dup := byNumber[num]; dup {
			return fmt.Errorf("%s: pb field number %d is used by both %s and %s", t.Name(), num, prev, f.Name)
		}
		byNumber[num] = f.Name

		fd := p.desc.Fields().ByNumber(protoreflect.FieldNumber(num))
		if fd == nil {
			return fmt.Errorf("%s.%s: field %d missing from descriptor", t.Name(), f.Name, num)
		}
		fp := fieldPlan{index: i, desc: fd}
		switch {
		case fd.IsMap():
			fp.kind = scalarMap
		case f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Uint8:
			fp.kind = scalarSingle
		case f.Type.Kind() == reflect.Ptr:
			fp.kind = messageSingle
			fp.elem = c.plans[f.Type.Elem()]
		case f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Struct:
			fp.kind = messageRepeated
			fp.elem = c.plans[f.Type.Elem()]
		case f.Type.Kind() == reflect.Slice:
			fp.kind = scalarRepeated
		default:
			fp.kind = scalarSingle
		}
		if (fp.kind == messageSingle || fp.kind == messageRepeated) && fp.elem == nil {
			return fmt.Errorf("%s.%s: no plan for nested message type", t.Name(), f.Name)
		}
		p.fields = append(p.fields, fp)
	}
	return nil
}

// describeMessage builds the DescriptorProto for one Go message struct.
func describeMessage(pkg string, t reflect.Type) (*descriptorpb.DescriptorProto, error) {
	dp := &descriptorpb.DescriptorProto{Name: strPtr(t.Name())}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag, ok := f.Tag.Lookup("pb")
		if !ok {
			continue
		}
		num, err := strconv.Atoi(tag)
		if err != nil {
			return nil, fmt.Errorf("%s.%s: pb tag %q is not a number", t.Name(), f.Name, tag)
		}
		name := CamelToSnake(f.Name)
		fd := &descriptorpb.FieldDescriptorProto{
			Name:     strPtr(name),
			JsonName: strPtr(name),
			Number:   int32Ptr(int32(num)),
			Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}
		ft := f.Type
		switch {
		// []byte is a scalar bytes field, not a repeated one, so it is checked
		// before the general slice case.
		case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Uint8:
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()

		case ft.Kind() == reflect.Map:
			// proto3 forbids float, bytes, and message map keys; the contract
			// narrows that to string keys, the only kind points use.
			if ft.Key().Kind() != reflect.String {
				return nil, fmt.Errorf("%s.%s: map keys must be strings", t.Name(), f.Name)
			}
			valType, err := scalarType(ft.Elem())
			if err != nil {
				return nil, fmt.Errorf("%s.%s: only scalar map values are supported: %w", t.Name(), f.Name, err)
			}
			// A proto3 map is sugar for a repeated nested entry message; the
			// descriptor has to spell that out.
			entry := &descriptorpb.DescriptorProto{
				Name:    strPtr(SnakeToGoCamel(name) + "Entry"),
				Options: &descriptorpb.MessageOptions{MapEntry: boolPtr(true)},
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: strPtr("key"), JsonName: strPtr("key"), Number: int32Ptr(1),
						Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
					},
					{
						Name: strPtr("value"), JsonName: strPtr("value"), Number: int32Ptr(2),
						Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:  valType.Enum(),
					},
				},
			}
			dp.NestedType = append(dp.NestedType, entry)
			fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = strPtr("." + pkg + "." + t.Name() + "." + entry.GetName())

		case ft.Kind() == reflect.Ptr:
			if ft.Elem().Kind() != reflect.Struct {
				return nil, fmt.Errorf("%s.%s: unsupported pointer field (only *Message is allowed)", t.Name(), f.Name)
			}
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = strPtr("." + pkg + "." + ft.Elem().Name())

		case ft.Kind() == reflect.Slice && ft.Elem().Kind() == reflect.Struct:
			fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
			fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
			fd.TypeName = strPtr("." + pkg + "." + ft.Elem().Name())

		case ft.Kind() == reflect.Slice:
			st, err := scalarType(ft.Elem())
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name(), f.Name, err)
			}
			fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
			fd.Type = st.Enum()

		default:
			st, err := scalarType(ft)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.Name(), f.Name, err)
			}
			fd.Type = st.Enum()
		}
		dp.Field = append(dp.Field, fd)
	}
	return dp, nil
}

// messageElem returns the message struct type a field refers to, if any: the
// pointee of *Message or the element of []Message. A []byte is bytes, not a
// repeated message, and a map value is always scalar, so neither yields one.
func messageElem(t reflect.Type) (reflect.Type, bool) {
	switch t.Kind() {
	case reflect.Ptr:
		if t.Elem().Kind() == reflect.Struct {
			return t.Elem(), true
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Struct {
			return t.Elem(), true
		}
	}
	return nil, false
}

func scalarType(t reflect.Type) (descriptorpb.FieldDescriptorProto_Type, error) {
	switch t.Kind() {
	case reflect.String:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING, nil
	case reflect.Bool:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL, nil
	case reflect.Int32:
		return descriptorpb.FieldDescriptorProto_TYPE_INT32, nil
	case reflect.Int64:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64, nil
	case reflect.Uint32:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT32, nil
	case reflect.Uint64:
		return descriptorpb.FieldDescriptorProto_TYPE_UINT64, nil
	case reflect.Float32:
		return descriptorpb.FieldDescriptorProto_TYPE_FLOAT, nil
	case reflect.Float64:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, nil
	case reflect.Int, reflect.Uint:
		// Proto has no width-ambiguous integer. Picking one silently would make
		// the wire contract depend on the daemon's word size, so the contract has
		// to name a width.
		return 0, fmt.Errorf("%s has no fixed width on the wire; use a sized integer such as int32, int64, uint32, or uint64", t.Kind())
	}
	return 0, fmt.Errorf("unsupported field type %s", t)
}

func strPtr(s string) *string { return &s }
func int32Ptr(v int32) *int32 { return &v }
func boolPtr(v bool) *bool    { return &v }

// CamelToSnake converts a Go field name to a proto3 snake_case field name,
// treating an initialism run as a single word: ContainerID -> container_id,
// HTTPServer -> http_server, APIKey -> api_key. A word boundary is inserted
// before an uppercase letter that either follows a lowercase or digit, or begins
// a new word after an acronym (i.e. it is itself followed by a lowercase).
//
// A lone trailing lowercase "s" is treated as a plural suffix on the acronym
// rather than the start of a new word, so ContainerIDs -> container_ids and CPUs
// -> cpus rather than container_i_ds / cp_us.
func CamelToSnake(s string) string {
	r := []rune(s)
	var b strings.Builder
	for i, c := range r {
		if i > 0 && c >= 'A' && c <= 'Z' {
			prev := r[i-1]
			prevIsLowerOrDigit := (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9')
			nextIsLower := i+1 < len(r) && r[i+1] >= 'a' && r[i+1] <= 'z'
			pluralS := nextIsLower && r[i+1] == 's' && (i+2 == len(r) || (r[i+2] >= 'A' && r[i+2] <= 'Z'))
			if prevIsLowerOrDigit || (prev >= 'A' && prev <= 'Z' && nextIsLower && !pluralS) {
				b.WriteByte('_')
			}
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b.WriteRune(c)
	}
	return b.String()
}

// SnakeToGoCamel converts a snake_case proto field name to the CamelCase name
// protobuf uses for a synthesized map entry message (labels -> Labels,
// port_bindings -> PortBindings).
func SnakeToGoCamel(s string) string {
	var b strings.Builder
	for _, part := range strings.Split(s, "_") {
		if part == "" {
			continue
		}
		b.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return b.String()
}

func snake(s string) string { return CamelToSnake(s) }

// PointDef is the part of an extension point that a wire contract is derived
// from: its id, which is also the proto package, and its provider interface,
// which supplies the methods and message types. [extensions.Point] satisfies it.
type PointDef interface {
	ID() extensions.PointID
	Interface() reflect.Type
}

// MustContract derives the contract for a point, panicking if the point's Go
// types cannot be represented on the wire.
//
// It is meant for a package-scope variable in the point's own package, so a
// contract this daemon cannot represent fails at build time, in the package that
// owns the mistake, rather than when some extension first declares the point.
func MustContract(p PointDef, service string) *Contract {
	c, err := NewContractFor(string(p.ID()), service, p.Interface())
	if err != nil {
		panic(fmt.Sprintf("extensions/wire: point %q: %v", p.ID(), err))
	}
	return c
}
