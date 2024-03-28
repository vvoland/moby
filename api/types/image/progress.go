package image

type ConvertProgressKind string

const (
	ConvertProgressKindConvert ConvertProgressKind = "convert"
)

type ConvertProgress struct {
	Kind string `json:"kind"`
	// Current is the current progress of the operation.
	Current int64 `json:"current,omitempty"`
}
